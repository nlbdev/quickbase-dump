package no.nlb.quickbase.dump;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Dumps a table from Quickbase to a XML file
 */
public class QuickbaseTableDump {
    
    private static final int MAX_ROWS_PER_REQUEST = 100;
    private static final String ENCODING = "iso-8859-1";
    private static final boolean DEBUG = !("".equals(System.getenv("QUICKBASE_DEBUG")) || System.getenv("QUICKBASE_DEBUG") == null);
    
    public static class QuickbaseClient {
        private HttpClient client;
        private String apptoken;
        private String url;
        private String ticket = null;
        
        public QuickbaseClient(String apptoken, String domain, String table, String username, String password) {
            this.client = HttpClientBuilder.create().build();
            this.apptoken = apptoken;
            this.url = "https://"+domain+"/db/main";
            
            QuickbaseRequest authRequest = newRequest("API_Authenticate");
            authRequest.setParameter("username", username);
            authRequest.setParameter("password", password);
            authRequest.setParameter("hours", "24");
            QuickbaseResponse response = authRequest.send();
            
            ticket = response.get("ticket");
            
            this.url = "https://"+domain+"/db/"+table;
        }
        
        public QuickbaseRequest newRequest(String action) {
            QuickbaseRequest request = new QuickbaseRequest(client, url, action);
            
            request.setParameter("encoding", ENCODING);
            request.setParameter("apptoken", apptoken);
            if (ticket != null) {
                request.setParameter("ticket", ticket);
            }
            
            return request;
        }
    }
    
    private static class QuickbaseRequest {
        Map<String,String> parameters;
        HttpClient client;
        String url;
        String action;
        
        public QuickbaseRequest(HttpClient client, String url, String action) {
            parameters = new HashMap<String,String>();
            this.client = client;
            this.url = url;
            this.action = action;
        }
        
        public QuickbaseResponse send() {
            String postString = "<qdbapi>";
            for (String key : parameters.keySet()) {
                postString += "<"+key+">"; // assume key is valid QName
                postString += parameters.get(key).replaceAll("<", "&lt;").replaceAll("<", "&gt;").replaceAll("&", "&amp;");
                postString += "</"+key+">";
            }
            postString += "</qdbapi>";
            byte[] postBytes = null;
            try {
                postBytes = postString.getBytes(ENCODING);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                System.exit(1);
            }
            
            HttpPost post = new HttpPost(url);
            post.setHeader("QUICKBASE-ACTION", action);
            post.setHeader(HttpHeaders.CONTENT_TYPE, "application/xml");
            HttpEntity postEntity = new ByteArrayEntity(postBytes);
            post.setEntity(postEntity);
            
            HttpResponse response = null;
            try {
                long timeBefore = new Date().getTime();
                if (DEBUG) {
                    System.err.println("Sending HTTP request...");
                }
                response = client.execute(post);
                if (DEBUG) {
                    long timeAfter = new Date().getTime();
                    System.err.println("HTTP request duration in ms: "+(timeAfter - timeBefore));
                }
                
            } catch (IOException|ParseException e) {
                e.printStackTrace();
                System.exit(1);
            }
            
            try {
                HttpEntity entity = response.getEntity();
                return new QuickbaseResponse(removeControlCharacters(EntityUtils.toString(entity,ENCODING)));
            } catch (IOException|ParseException e) {
                e.printStackTrace();
                System.exit(1);
            }
            return null;
        }
        
        public static String removeControlCharacters(String value) {
            if (DEBUG) {
                System.err.println("Removing control characters...");
            }
            String newValue = "";
            final int length = value.length();
            for (int offset = 0; offset < length; ) {
               final int codepoint = value.codePointAt(offset);
               if (codepoint <= 8 ||
                   codepoint >= 14 && codepoint <= 31 ||
                   codepoint >= 128 && codepoint <= 132 ||
                   codepoint >= 134 && codepoint <= 159) {
                   if (DEBUG) {
                       System.err.println("removing codepoint: "+codepoint);
                   }
               } else {
                   newValue += value.charAt(offset);
               }
               offset += Character.charCount(codepoint);
            }
            if (DEBUG) {
                System.err.println("Removing control characters... done");
            }
            return newValue;
        }
        
        public void setParameter(String key, String value) {
            parameters.put(key, value);
        }
    }
    
    private static class QuickbaseResponse {
        public String responseString = null;
        private Document xml = null;
        private Map<String,String> results = null;
        
        public QuickbaseResponse(String responseString) {
            this.responseString = responseString;
        }
        
        private void parseResults() {
            if (results != null) return;
            String s = new String(responseString);
            if (DEBUG) {
                System.err.println("Parsing results...");
            }
            
            results = new HashMap<String,String>();
            NodeList qdbapiNodeList = xml().getElementsByTagName("qdbapi");
            Element qdbapiElement = (Element)qdbapiNodeList.item(0);
            NodeList resultNodes = qdbapiElement.getChildNodes();
            for (int i = 0; i < resultNodes.getLength(); i++) { // iterate elements inside element
                Node resultChildNode = resultNodes.item(i);
                if (resultChildNode.getNodeType() == Node.ELEMENT_NODE) {
                    String name = resultChildNode.getNodeName();
                    String value = null;
                    NodeList childNodes = resultChildNode.getChildNodes();
                    for (int j = 0; j < childNodes.getLength(); j++) { // iterate nodes inside element to check for content
                        Node childNode = childNodes.item(j);
                        if (childNode.getNodeType() == Node.ELEMENT_NODE) {
                            // ignore elements with complex content
                            value = null;
                            break;
                        }
                        if (childNode.getNodeType() == Node.TEXT_NODE) {
                            value = childNode.getNodeValue();
                        }
                    }
                    if (value != null) {
                        results.put(name, value);
                    }
                }
            }
            
            if (DEBUG) {
                System.err.println("Parsing results... done");
            }
        }
        
        public Document xml() {
            if (xml != null) return xml;
            if (DEBUG) {
                System.err.println("Parsing xml...");
            }
            
            try {
                DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
                InputStream stream = new ByteArrayInputStream(responseString.getBytes(ENCODING));
                xml = documentBuilder.parse(stream);
                
            } catch (ParserConfigurationException | SAXException | IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
            
            if (DEBUG) {
                System.err.println("Parsing xml... done");
            }
            
            return xml;
        }
        
        public String get(String key) {
            parseResults();
            return results.get(key);
        }
        
        public Map<String,Map<String,String>> getFields() {
            if (DEBUG) {
                System.err.println("Getting fields...");
            }
            
            Map<String,Map<String,String>> fields = new HashMap<String,Map<String,String>>();
            NodeList fieldElements = xml().getElementsByTagName("field");
            for (int i = 0; i < fieldElements.getLength(); i++) {
                Element fieldElement = (Element)fieldElements.item(i);
                
                Map<String,String> fieldValues = new HashMap<String,String>();
                
                NodeList fieldChildNodes = fieldElement.getChildNodes();
                for (int j = 0; j < fieldChildNodes.getLength(); j++) { // iterate elements inside field element
                    Node fieldChildNode = fieldChildNodes.item(j);
                    if (fieldChildNode.getNodeType() == Node.ELEMENT_NODE) {
                        String name = fieldChildNode.getNodeName();
                        String value = null;
                        NodeList childNodes = fieldChildNode.getChildNodes();
                        for (int k = 0; k < childNodes.getLength(); k++) { // iterate nodes inside field value element to check for content
                            Node childNode = childNodes.item(k);
                            if (childNode.getNodeType() == Node.ELEMENT_NODE) {
                                // ignore elements with complex content
                                value = null;
                                break;
                            }
                            if (childNode.getNodeType() == Node.TEXT_NODE) {
                                value = childNode.getNodeValue();
                            }
                        }
                        if (value != null) {
                            fieldValues.put(name, value);
                        }
                    }
                }
                
                fieldValues.put("base_type", fieldElement.getAttribute("base_type"));
                fieldValues.put("field_type", fieldElement.getAttribute("field_type"));
                fieldValues.put("mode", fieldElement.getAttribute("mode"));
                fieldValues.put("role", fieldElement.getAttribute("role"));
                
                fields.put(fieldElement.getAttribute("id"), fieldValues);
            }
            
            if (DEBUG) {
                System.err.println("Getting fields... done");
            }
            
            return fields;
        }
        
        public String getRecordIdId() {
            Map<String,Map<String,String>> fields = getFields();
            for (String id : fields.keySet()) {
                if ("recordid".equals(fields.get(id).get("role"))) {
                    return id;
                }
            }
            return null;
        }

        public Map<String,Map<String,String>> getRecords() {
            if (DEBUG) {
                System.err.println("Getting records...");
            }
            
            Map<String,Map<String,String>> records = new HashMap<String,Map<String,String>>();
            NodeList recordElements = xml().getElementsByTagName("record");
            for (int i = 0; i < recordElements.getLength(); i++) {
                Element recordElement = (Element)recordElements.item(i);
                
                Map<String,String> recordValues = new HashMap<String,String>();
                
                NodeList recordChildNodes = recordElement.getChildNodes();
                for (int j = 0; j < recordChildNodes.getLength(); j++) { // iterate elements inside record element
                    Node recordChildNode = recordChildNodes.item(j);
                    if (recordChildNode.getNodeType() == Node.ELEMENT_NODE) {
                        String name = recordChildNode.getNodeName();
                        if ("f".equals(name)) {
                            name = ((Element)recordChildNode).getAttribute("id");
                        }
                        String value = null;
                        NodeList childNodes = recordChildNode.getChildNodes();
                        for (int k = 0; k < childNodes.getLength(); k++) { // iterate nodes inside record value element to check for content
                            Node childNode = childNodes.item(k);
                            if (childNode.getNodeType() == Node.ELEMENT_NODE) {
                                // ignore elements with complex content
                                value = null;
                                break;
                            }
                            if (childNode.getNodeType() == Node.TEXT_NODE) {
                                try {
                                    value = childNode.getNodeValue();
                                    
                                } catch (DOMException e) {
                                    e.printStackTrace();
                                    System.exit(1);
                                }
                            }
                        }
                        if (value != null) {
                            recordValues.put(name, value);
                        }
                    }
                }
                
                records.put(recordElement.getAttribute("rid"), recordValues);
            }
            
            if (DEBUG) {
                System.err.println("Getting records... done");
            }
            
            return records;
        }
    }
    
    public static void main(String[] args) {
        String appToken = System.getenv("QUICKBASE_APP_TOKEN");
        String domain = System.getenv("QUICKBASE_DOMAIN");
        String username = System.getenv("QUICKBASE_USERNAME");
        String password = System.getenv("QUICKBASE_PASSWORD");
        String table = System.getenv("QUICKBASE_TABLE");
        
        if (appToken == null || "".equals(appToken)) {
            System.err.println("Missing environment variable: QUICKBASE_APP_TOKEN");
            System.exit(1);
            
        } else if (domain == null || "".equals(domain)) {
            System.err.println("Missing environment variable: QUICKBASE_DOMAIN");
            System.exit(1);
            
        } else if (username == null || "".equals(username)) {
            System.err.println("Missing environment variable: QUICKBASE_USERNAME");
            System.exit(1);
            
        } else if (password == null || "".equals(password)) {
            System.err.println("Missing environment variable: QUICKBASE_PASSWORD");
            System.exit(1);
            
        } else if (table == null || "".equals(table)) {
            System.err.println("Missing environment variable: QUICKBASE_TABLE");
            System.exit(1);
        }
        
        QuickbaseClient client = new QuickbaseClient(appToken, domain, table, username, password);
        QuickbaseRequest request;
        QuickbaseResponse response, schema;
        Map<String, Map<String, String>> records;
        String combinedResponse = "";
        
        // find id of row containing record id
        request = client.newRequest("API_GetSchema");
        schema = request.send();
        String recordIdId = schema.getRecordIdId();
        
        // find lowest record id
        request = client.newRequest("API_DoQuery");
        request.setParameter("query", "");
        request.setParameter("clist", recordIdId);
        request.setParameter("slist", recordIdId);
        request.setParameter("options", "sortorder-A.num-1");
        request.setParameter("includeRids", "1");
        request.setParameter("fmt", "structured");
        response = request.send();
        records = response.getRecords();
        Integer startRecordId = null;
        for (String recordId : records.keySet()) {
            startRecordId = new Integer(recordId);
            assert(startRecordId != null);
        }
        if (DEBUG) {
            System.err.println("startRecordId: "+startRecordId);
        }
        
        // find highest record id
        request = client.newRequest("API_DoQuery");
        request.setParameter("query", "");
        request.setParameter("clist", recordIdId);
        request.setParameter("slist", recordIdId);
        request.setParameter("options", "sortorder-D.num-1");
        request.setParameter("includeRids", "1");
        request.setParameter("fmt", "structured");
        response = request.send();
        records = response.getRecords();
        Integer endRecordId = null;
        for (String recordId : records.keySet()) {
            endRecordId = new Integer(recordId);
            assert(endRecordId != null);
        }
        if (DEBUG) {
            System.err.println("endRecordId: "+endRecordId);
        }
        
        for (int page = 0; startRecordId + page * MAX_ROWS_PER_REQUEST <= endRecordId; page++) {
            int from = startRecordId + page * MAX_ROWS_PER_REQUEST;
            int to = startRecordId + (page+1) * MAX_ROWS_PER_REQUEST;
            
            request = client.newRequest("API_DoQuery");
            request.setParameter("query", "{'"+recordIdId+"'.GTE.'"+from+"'}AND{'"+recordIdId+"'.LT.'"+to+"'}");
            request.setParameter("clist", "a");
            request.setParameter("slist", recordIdId);
            request.setParameter("includeRids", "1");
            request.setParameter("fmt", "structured");
            response = request.send();
            if (DEBUG) {
                System.err.println("found "+response.getRecords().size()+" records in record id range ["+from+","+to+")");
            }
            
            if ("".equals(combinedResponse)) {
                combinedResponse += response.responseString.replaceAll("(?s)(<records[^>]*>[^<]*).*", "$1");
            }
            combinedResponse += response.responseString.replaceAll("(?s).*<records[^>]*>[^<]*", "").replaceAll("(?s)</records.*", "");
        }
        if (!"".equals(combinedResponse)) {
            combinedResponse += response.responseString.replaceAll("(?s).*(</records)", "$1");
        }
        
        if (DEBUG) {
            response = new QuickbaseResponse(combinedResponse);
            System.err.println("Found a total of "+response.getRecords().size()+" records");
        }
        
        System.out.println(combinedResponse);
    }
}
