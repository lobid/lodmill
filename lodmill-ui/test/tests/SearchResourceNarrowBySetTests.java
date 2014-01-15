/* Copyright 2014 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package tests;

import static org.fest.assertions.Assertions.assertThat;
import static play.test.Helpers.running;

import java.util.List;

import models.Document;
import models.Index;
import models.Parameter;
import models.Search;

import org.junit.Test;

import play.libs.Json;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Tests for searching resources by author names, narrowed to set membership.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public class SearchResourceNarrowBySetTests extends SearchTestsHarness {

	final String SET_FULL = "http://lobid.org/resource/NWBib";
	final String SET_SHORT = "NWBib";

	/*@formatter:off*/
	@Test public void searchUrlShort() { searchUrlStyle(SET_SHORT); }
	@Test public void searchUrlFull() { searchUrlStyle(SET_FULL); }
	@Test public void searchApiShort() { searchApiStyle(SET_SHORT); }
	@Test public void searchApiFull() { searchApiStyle(SET_FULL); }
	/*@formatter:on*/

	private void searchUrlStyle(final String set) {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				String response = call("resource?author=Hu&set=" + set);
				assertThat(response).isNotNull();
				final JsonNode jsonObjectIds = Json.parse(response);
				assertThat(jsonObjectIds.isArray()).isTrue();
				assertThat(jsonObjectIds.size()).isEqualTo(1);
				assertThat(jsonObjectIds.get(0).toString()).contains(SET_FULL);
			}
		});
	}

	private void searchApiStyle(final String set) {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final List<Document> docs =
						new Search("Hu", Index.LOBID_RESOURCES, Parameter.AUTHOR).set(set)
								.documents();
				assertThat(docs.size()).isEqualTo(1);
				assertThat(docs.get(0).getSourceWithFullProperties())
						.contains(SET_FULL);
			}
		});
	}
}
