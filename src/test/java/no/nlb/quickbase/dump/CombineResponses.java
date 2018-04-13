package no.nlb.quickbase.dump;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class CombineResponses {

	@Test
	public void test() {
		List<QuickbaseTableDump.QuickbaseResponse> responses = new ArrayList<QuickbaseTableDump.QuickbaseResponse>();
		responses.add(new QuickbaseTableDump.QuickbaseResponse("<qdbapi>\n<common-outer/>\n<table>\n<common-inner/>\n<lusers>\n<luser id=\"a\">a@a.a</luser>\n<luser id=\"b\">b@b.b</luser>\n</lusers>\n<records>\n<record rid=\"1\"/>\n</records>\n</table>\n</qdbapi>\n"));
		responses.add(new QuickbaseTableDump.QuickbaseResponse("<qdbapi>\n<common-outer/>\n<table>\n<common-inner/>\n<lusers>\n<luser id=\"a\">a@a.a</luser>\n<luser id=\"c\">c@c.c</luser>\n</lusers>\n<records>\n<record rid=\"2\"/>\n</records>\n</table>\n</qdbapi>\n"));
		String combinedResponse = QuickbaseTableDump.combineResponses(responses);
		
		assertEquals("Quickbase users and records should merge properly",
					 "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<qdbapi>\n<common-outer/>\n<table>\n<common-inner/>\n<lusers>\n<luser id=\"a\">a@a.a</luser>\n<luser id=\"b\">b@b.b</luser>\n<luser id=\"c\">c@c.c</luser>\n</lusers>\n      <records>\n<record rid=\"1\"/>\n<record rid=\"2\"/>\n      </records>\n    </table>\n</qdbapi>\n",
					 combinedResponse);
	}

}
