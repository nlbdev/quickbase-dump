package no.nlb.quickbase.dump;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

/**
 * Dumps a table from Quickbase to a XML file
 */
public class QuickbaseTableDump {
	
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
				postBytes = postString.getBytes("UTF-8");
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
				response = client.execute(post);
				
			} catch (IOException|ParseException e) {
				e.printStackTrace();
				System.exit(1);
			}
			
			try {
				return new QuickbaseResponse(EntityUtils.toString(response.getEntity()));
			} catch (IOException|ParseException e) {
				e.printStackTrace();
				System.exit(1);
			}
			return null;
		}
		
		public void setParameter(String key, String value) {
			parameters.put(key, value);
		}
	}
	
	private static class QuickbaseResponse {
		public String responseString = null;
		private Map<String,String> results = null;
		
		public QuickbaseResponse(String responseString) {
			this.responseString = responseString;
		}
		
		private void parseResults() {
			if (results != null) return;
			String s = new String(responseString);
			
			results = new HashMap<String,String>();
			while (s.length() > 0) {
				s = s.replaceAll("(?s)^.*?<([a-z])", "$1");
				String key = s.replaceAll("(?s)>.*", "").replaceAll("\\s","");
				if ("qdbapi".equals(key)) continue;
				if (key.contains("<")) {
					s = s.replaceAll("(?s)^.*?<[^>]*", "");
					continue;
				}
				if ("".equals(key)) break;
				s = s.replaceAll("(?s)^[^>]*>", "");
				String value = s.replaceAll("(?s)<.*","");
				results.put(key, value);
				s = s.replaceAll("(?s)^.*?</[^>]*>", "");
			}
		}
		
		public String get(String key) {
			parseResults();
			return results.get(key);
		}
	}
	
    public static void main(String[] args) {
    	String appToken = System.getenv("QUICKBASE_APP_TOKEN");
        String domain = System.getenv("QUICKBASE_DOMAIN");
        String username = System.getenv("QUICKBASE_USERNAME");
        String password = System.getenv("QUICKBASE_PASSWORD");
        String table = System.getenv("QUICKBASE_TABLE");
        
        QuickbaseClient client = new QuickbaseClient(appToken, domain, table, username, password);
        QuickbaseRequest request;
        QuickbaseResponse response;
        
        //request = client.newRequest("API_GetSchema");
        //response = request.send();
        //System.out.println(response.responseString);
        
        request = client.newRequest("API_DoQuery");
        request.setParameter("query", "");
        request.setParameter("clist", "a");
        request.setParameter("includeRids", "1");
        request.setParameter("fmt", "structured");
        response = request.send();
        System.out.println(response.responseString);
        
    }
}
