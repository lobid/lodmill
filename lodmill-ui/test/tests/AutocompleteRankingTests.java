/* Copyright 2014 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package tests;

import static org.fest.assertions.Assertions.assertThat;
import static play.test.Helpers.running;

import org.junit.Test;

import play.libs.Json;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Tests for ranking autocomplete suggestions.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public class AutocompleteRankingTests extends SearchTestsHarness {

	/*@formatter:off*/
	@Test public void personFirstLastFull() { searchGndOrdering("thomas mann"); }
	@Test public void personLastFirstFull() { searchGndOrdering("mann, thomas"); }
	@Test public void personFirstLastPart() { searchGndOrdering("thomas ma"); }
	@Test public void personLastFirstPart() { searchGndOrdering("mann, tho"); }
	/*@formatter:on*/

	public void searchGndOrdering(final String query) {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final JsonNode jsonObject =
						Json.parse(call("person?name=" + query + "&format=short"));
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.size()).isEqualTo(1);
				assertThat(jsonObject.get(0).asText()).isEqualTo(
						"Mann, Thomas (1875-1955)");
			}
		});
	}
}
