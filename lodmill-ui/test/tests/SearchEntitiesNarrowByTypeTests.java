/* Copyright 2014 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package tests;

import static org.fest.assertions.Assertions.assertThat;
import static play.test.Helpers.running;

import org.junit.Test;

import play.libs.Json;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Tests for searching entities, narrowed to their types.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public class SearchEntitiesNarrowByTypeTests extends SearchTestsHarness {

	//@formatter:off
	@Test public void resources() { search("resource?q=*", 25); }
	@Test public void bibliographicResources() { search("resource?q=*&t=BibliographicResource", 25); }
	@Test public void books() { search("resource?q=*&t=Book", 20); }
	@Test public void journals() { search("resource?q=*&t=Journal", 1); }
	@Test public void newspapers() { search("resource?q=*&t=Newspaper", 1); }
	@Test public void differentiatedPersons() { search("person?q=*&t=DifferentiatedPerson", 9); }
	@Test public void undifferentiatedPersons() { search("person?q=*&t=UndifferentiatedPerson", 4); }
	@Test public void subjects() { search("subject?q=*", 17); }
	@Test public void journalsUri() { search("resource?q=*&t=http://purl.org/ontology/bibo/Journal", 1); }
	@Test public void newspapersUri() { search("resource?q=*&t=http://purl.org/ontology/bibo/Newspaper", 1); }
	//@formatter:on

	private static void search(final String q, final int hits) {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				String response = call(q);
				assertThat(response).isNotNull();
				final JsonNode jsonObjectIds = Json.parse(response);
				assertThat(jsonObjectIds.isArray()).isTrue();
				assertThat(jsonObjectIds.size()).isEqualTo(hits);
			}
		});
	}
}
