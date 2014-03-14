/* Copyright 2012-2014 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package tests;

import static org.fest.assertions.Assertions.assertThat;
import static play.mvc.Http.Status.BAD_REQUEST;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.GET;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.fakeRequest;
import static play.test.Helpers.route;
import static play.test.Helpers.running;
import static play.test.Helpers.status;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import models.Document;
import models.Index;
import models.Parameter;
import models.Search;

import org.junit.Test;

import play.libs.Json;
import play.mvc.Result;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * Tests for the search functionality.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public class SearchTests extends SearchTestsHarness {

	@Test
	public void accessIndex() {
		assertThat(
				client.prepareSearch().execute().actionGet().getHits().totalHits())
				.isEqualTo(50);
		JsonNode json =
				Json.parse(client
						.prepareGet(Index.LOBID_RESOURCES.id(), "json-ld-lobid",
								"http://lobid.org/resource/BT000001260").execute().actionGet()
						.getSourceAsString());
		assertThat(json.isObject()).isTrue();
		assertThat(
				json.findValue("http://d-nb.info/standards/elementset/gnd#dateOfBirth")
						.findValue("@value").toString()).isEqualTo("\"1906\"");
	}

	@Test
	public void searchViaModel() {
		final List<Document> docs =
				new Search("theo", Index.LOBID_RESOURCES, Parameter.AUTHOR).documents();
		assertThat(docs.size()).isPositive();
		for (Document document : docs) {
			assertThat(document.getMatchedField().toLowerCase()).contains("theo");
		}
	}

	@Test
	public void searchViaModelOrgName() {
		assertThat(searchOrgByName("hbz Land")).isEqualTo(1);
		assertThat(searchOrgByName("hbz Schmeckermeck")).isEqualTo(0);
	}

	private static int searchOrgByName(final String term) {
		return new Search(term, Index.LOBID_ORGANISATIONS, Parameter.NAME)
				.documents().size();
	}

	@Test
	public void searchViaModelOrgQuery() {
		assertThat(searchOrgQuery("Einrichtung ohne Bestand")).isEqualTo(1);
		assertThat(searchOrgQuery("hbz Schmeckermeck")).isEqualTo(1);
	}

	private static int searchOrgQuery(final String term) {
		return new Search(term, Index.LOBID_ORGANISATIONS, Parameter.Q).documents()
				.size();
	}

	/*@formatter:off*/
	@Test public void searchViaModelOrgIdShort() { searchOrgById("DE-605"); }
	@Test public void searchViaModelOrgIdLong() { searchOrgById("http://lobid.org/organisation/DE-605"); }
	/*@formatter:on*/

	private static void searchOrgById(final String term) {
		final List<Document> docs =
				new Search(term, Index.LOBID_ORGANISATIONS, Parameter.ID).documents();
		assertThat(docs.size()).isEqualTo(1);
	}

	/*@formatter:off*/
	@Test public void searchResByIdTT() { searchResById("TT002234003"); }
	@Test public void searchResByIdHT() { searchResById("HT002189125"); }
	@Test public void searchResByIdZdb1() { searchResById("ZDB2615620-9"); }
	@Test public void searchResByIdZdb2() { searchResById("ZDB2530091-X"); }
	@Test public void searchResByIdTTUrl() { searchResById("http://lobid.org/resource/TT002234003"); }
	@Test public void searchResByIdHTUrl() { searchResById("http://lobid.org/resource/HT002189125"); }
	@Test public void searchResByIdZdbUrl1() { searchResById("http://lobid.org/resource/ZDB2615620-9"); }
	@Test public void searchResByIdZdbUrl2() { searchResById("http://lobid.org/resource/ZDB2530091-X"); }
	@Test public void searchResByIdISBN() { searchResById("0940450003"); }
	@Test public void searchResByIdUrn() { searchResById("urn:nbn:de:101:1-201210094953"); }
	/*@formatter:on*/

	private static void searchResById(final String term) {
		final List<Document> docs =
				new Search(term, Index.LOBID_RESOURCES, Parameter.ID).documents();
		assertThat(docs.size()).isEqualTo(1);
	}

	@Test
	public void searchResByIdWithReturnFieldViaModel() {
		running(fakeApplication(), new Runnable() {
			@Override
			public void run() {
				final List<Document> docs =
						new Search("TT050326640", Index.LOBID_RESOURCES, Parameter.ID)
								.field("fulltextOnline").documents();
				assertThat(docs.size()).isEqualTo(1);
				assertThat(docs.get(0).getSource()).isEqualTo(
						"[\"http://dx.doi.org/10.1007/978-1-4020-8389-1\"]");
			}
		});
	}

	/*@formatter:off*/
	@Test public void returnFieldParam() { returnFieldHit("resource?id=TT050326640&"); }
	@Test public void returnFieldPath() { returnFieldHit("resource/TT050326640?"); }
	/*@formatter:on*/

	public void returnFieldHit(final String query) {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final String response = call(query + "format=short.fulltextOnline");
				assertThat(response).isNotNull();
				final JsonNode jsonObject = Json.parse(response);
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.size()).isEqualTo(1);
				assertThat(jsonObject.elements().next().asText()).isEqualTo(
						"http://dx.doi.org/10.1007/978-1-4020-8389-1");
			}
		});
	}

	/*@formatter:off*/
	@Test public void returnFieldParamNoHit() { returnFieldNoHit("resource?id=HT000000716&"); }
	@Test public void returnFieldPathNoHit() { returnFieldNoHit("resource/HT000000716?"); }
	/*@formatter:on*/

	public void returnFieldNoHit(final String query) {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final String response = call(query + "format=short.fulltextOnline");
				assertThat(response).isNotNull();
				final JsonNode jsonObject = Json.parse(response);
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.size()).isEqualTo(0);
			}
		});
	}

	@Test
	public void returnFieldSorting() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final String response =
						call("resource?author=abraham&format=short.issued");
				assertThat(response).isNotNull();
				final JsonNode jsonObject = Json.parse(response);
				assertThat(jsonObject.isArray()).isTrue();
				final Iterator<JsonNode> elements = jsonObject.elements();
				assertThat(elements.next().asText()).isEqualTo("1719");
				assertThat(elements.next().asText()).isEqualTo("1906");
				assertThat(elements.next().asText()).isEqualTo("1973");
				assertThat(elements.next().asText()).isEqualTo("1976");
				assertThat(elements.next().asText()).isEqualTo("1977");
				assertThat(elements.next().asText()).isEqualTo("1979");
				assertThat(elements.next().asText()).isEqualTo("1981");
			}
		});
	}

	@Test
	public void returnFieldBadRequest() {
		running(fakeApplication(), new Runnable() {
			@Override
			public void run() {
				assertThat(
						status(route(fakeRequest(GET,
								"/resource?author=Böll&format=ids.issued")))).isEqualTo(
						BAD_REQUEST);
			}
		});
	}

	/*@formatter:off*/
	@Test public void searchViaModelBirth0() { findOneBy("Theo Hundt"); }
	@Test public void searchViaModelBirth1() { findOneBy("Hundt, Theo (1906-)"); }
	@Test public void searchViaModelBirth2() { findOneBy("Theo Hundt (1906-)"); }
	@Test public void searchViaModelBirth3() { findOneBy("Goeters, Johann F. Gerhard (1926-1996)"); }
	@Test public void searchViaModelMulti1() { findOneBy("Vollhardt, Kurt Peter C."); }
	@Test public void searchViaModelMulti2() { findOneBy("Kurt Peter C. Vollhardt"); }
	@Test public void searchViaModelMulti3() { findOneBy("Vollhardt, Kurt Peter C. (1946-)"); }
	@Test public void searchViaModelMulti4() { findOneBy("Neil Eric Schore (1948-)"); }
	@Test public void searchViaModelMulti5() { findOneBy("131392786"); }
	@Test public void searchViaModelMulti6() { findOneBy("http://d-nb.info/gnd/131392786"); }
	/*@formatter:on*/

	private static void findOneBy(String name) {
		assertThat(
				new Search(name, Index.LOBID_RESOURCES, Parameter.AUTHOR).documents()
						.size()).isEqualTo(1);
	}

	@Test
	public void searchViaModelMultiResult() {
		List<Document> documents =
				new Search("Neil Eric Schore (1948-)", Index.LOBID_RESOURCES,
						Parameter.AUTHOR).documents();
		assertThat(documents.size()).isEqualTo(1);
		assertThat(documents.get(0).getMatchedField()).isEqualTo(
				"Vollhardt, Kurt Peter C. (1946-)");
	}

	@Test
	public void searchViaModelSetNwBib() {
		List<Document> documents =
				new Search("NwBib", Index.LOBID_RESOURCES, Parameter.SET).documents();
		assertThat(documents.size()).isEqualTo(3);
		assertThat(documents.get(0).getMatchedField()).isEqualTo(
				"http://lobid.org/resource/NWBib");
	}

	@Test
	public void indexRoute() {
		running(fakeApplication(), new Runnable() {
			@Override
			public void run() {
				Result result = route(fakeRequest(GET, "/"));
				assertThat(status(result)).isEqualTo(OK);
			}
		});
	}

	@Test
	public void searchViaApiPageEmpty() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				assertThat(call("")).contains("<html");
			}
		});
	}

	@Test
	public void searchViaApiHtml() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				assertThat(call("resource?author=abraham", "text/html")).contains(
						"<html");
			}
		});
	}

	@Test
	public void searchViaApiFull() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final JsonNode jsonObject =
						Json.parse(call("resource?author=abraham&format=full"));
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.size()).isGreaterThan(5).isLessThan(10);
				assertThat(jsonObject.elements().next().isContainerNode()).isTrue();
			}
		});
	}

	@Test
	public void searchViaApiShort() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final JsonNode jsonObject =
						Json.parse(call("resource?author=abraham&format=short"));
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.size()).isGreaterThan(5).isLessThan(10);
				assertThat(jsonObject.elements().next().isContainerNode()).isFalse();
			}
		});
	}

	private static List<String> list(JsonNode jsonObject) {
		List<String> list = new ArrayList<>();
		Iterator<JsonNode> elements = jsonObject.elements();
		while (elements.hasNext()) {
			list.add(elements.next().asText());
		}
		return list;
	}

	@Test
	public void searchViaApiGnd() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final JsonNode jsonObject =
						Json.parse(call("person?name=bach&format=short&t="
								+ "http://d-nb.info/standards/elementset/gnd%23DifferentiatedPerson"));
				assertThat(jsonObject.isArray()).isTrue();
				/* differentiated & *starting* with 'bach' only & no dupes */
				assertThat(jsonObject.size()).isEqualTo(2);
			}
		});
	}

	/* @formatter:off */
	@Test public void searchAltNamePlain() { searchName("Schmidt, Loki", 1); }
	@Test public void searchAltNameSwap()  { searchName("Loki Schmidt", 1); }
	@Test public void searchAltNameSecond(){ searchName("Hannelore Glaser", 1); }
	@Test public void searchAltNameShort() { searchName("Loki", 1); }
	@Test public void searchAltNameNgram() { searchName("Lok", 1); }
	@Test public void searchPrefNameNgram(){ searchName("Hanne", 1); }
	@Test public void searchAltNameDates() { searchName("Loki Schmidt (1919-2010)", 1); }
	@Test public void searchAltNameBirth() { searchName("Loki Schmidt (1919-)", 1); }
	@Test public void searchAltNameNone()  { searchName("Loki Müller", 0); }
	/* @formatter:on */

	private static void searchName(final String name, final int results) {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final JsonNode jsonObject =
						Json.parse(call("person?name=" + name.replace(" ", "%20")
								+ "&format=short"));
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.size()).isEqualTo(results);
				if (results > 0) {
					assertThat(Iterables.any(list(jsonObject), new Predicate<String>() {
						@Override
						public boolean apply(String s) {
							return s.equals("Schmidt, Hannelore (1919-2010)");
						}
					})).isTrue();
				}
			}
		});
	}

	@Test
	public void searchViaApiResourcesAuthorId() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				String gndId = "118554808";
				final JsonNode jsonObject =
						Json.parse(call("resource?author=" + gndId));
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.size()).isEqualTo(1);
				assertThat(jsonObject.get(0).toString()).contains(
						"http://d-nb.info/gnd/" + gndId);
			}
		});
	}

	/* @formatter:off */
	@Test public void resourceByGndSubjectMulti(){resByGndSubject("44141956", 2);}
	@Test public void resourceByGndSubjectDashed(){resByGndSubject("4414195-6", 1);}
	@Test public void resourceByGndSubjectSingle(){resByGndSubject("189452846", 1);}
	/* @formatter:on */

	public void resByGndSubject(final String gndId, final int results) {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final JsonNode jsonObject =
						Json.parse(call("resource?subject=" + gndId));
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.size()).isEqualTo(results);
				assertThat(jsonObject.get(0).toString()).contains(
						"http://d-nb.info/gnd/" + gndId);
			}
		});
	}

	/* @formatter:off */
	@Test public void personByGndIdNumeric(){gndPerson("1019737174", 1);}
	@Test public void personByGndIdAlphaNumeric(){gndPerson("11850553X", 1);}
	@Test public void personByGndIdAlphaNumericPlusDash(){gndPerson("10115480-X", 1);}
	@Test public void personByGndIdNumericFull(){gndPerson("http://d-nb.info/gnd/1019737174", 1);}
	@Test public void personByGndIdAlphaNumericFull(){gndPerson("http://d-nb.info/gnd/11850553X", 1);}
	@Test public void personByGndIdAlphaNumericPlusDashFull(){gndPerson("http://d-nb.info/gnd/10115480-X", 1);}
	/* @formatter:on */

	public void gndPerson(final String gndId, final int results) {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final JsonNode jsonObject = Json.parse(call("person?id=" + gndId));
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.size()).isEqualTo(results);
				final String gndPrefix = "http://d-nb.info/gnd/";
				assertThat(jsonObject.get(0).toString()).contains(
						gndPrefix + gndId.replace(gndPrefix, ""));
			}
		});
	}

	/* @formatter:off */
	@Test public void subjectByGndId1Preferred(){gndSubject("Herbstadt-Ottelmannshausen", 1);}
	@Test public void subjectByGndId1PreferredNGram(){gndSubject("Ottel", 1);}
	@Test public void subjectByGndId1Variant(){gndSubject("Ottelmannshausen  Herbstadt ", 1);}
	@Test public void subjectByGndId1VariantNGram(){gndSubject("  Her", 1);}
	@Test public void subjectByGndId2Preferred(){gndSubject("Kirchhundem-Heinsberg", 1);}
	@Test public void subjectByGndId2Variant(){gndSubject("Heinsberg  Kirchhundem ", 1);}
	/* @formatter:on */

	public void gndSubject(final String subjectName, final int results) {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final JsonNode jsonObject =
						Json.parse(call("subject?name=" + subjectName));
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.size()).isEqualTo(results);
				assertThat(jsonObject.get(0).toString()).contains(subjectName);
			}
		});
	}

	/* @formatter:off */
	@Test public void itemByIdParam1(){findItem("item?id=BT000000079%3AGA+644");}
	@Test public void itemByIdParam2(){findItem("item?id=BT000001260%3AMA+742");}
	@Test public void itemByIdRoute(){findItem("item/BT000000079%3AGA+644");}
	@Test public void itemByIdUri1(){findItem("item?id=http://lobid.org/item/BT000000079%3AGA+644");}
	@Test public void itemByIdUri2(){findItem("item?id=http://lobid.org/item/BT000001260%3AMA+742");}
	@Test public void itemByName(){findItem("item?name=GA+644");}
	/* @formatter:on */

	public void findItem(final String call) {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final JsonNode jsonObject = Json.parse(call(call));
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.size()).isEqualTo(1);
			}
		});
	}

	private final static String ENDPOINT = "resource?author=abraham";

	@Test
	public void searchViaApiWithContentNegotiationNTriples() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final String response = call(ENDPOINT, "text/plain");
				assertThat(response).isNotEmpty().startsWith("<http");
				assertThat(response).contains(
						"<http://xmlns.com/foaf/0.1/primaryTopic>");
			}
		});
	}

	@Test
	public void searchViaApiWithContentNegotiationTurtle() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final String response = call(ENDPOINT, "text/turtle");
				assertThat(response).isNotEmpty().contains("      a       ");
				assertThat(response).contains(
						"<http://xmlns.com/foaf/0.1/primaryTopic>");
			}
		});
	}

	@Test
	public void searchViaApiWithContentNegotiationRdfa() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				assertThat(call(ENDPOINT, "text/html")).isNotEmpty().contains(
						"<!DOCTYPE html>");
			}
		});
	}

	@Test
	public void searchViaApiWithContentNegotiationRdfXml() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				assertThat(call(ENDPOINT, "application/rdf+xml")).isNotEmpty()
						.contains("<rdf:RDF");
			}
		});
	}

	@Test
	public void searchViaApiWithContentNegotiationN3() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final String turtle = call(ENDPOINT, "text/turtle");
				final String n3 = call(ENDPOINT, "text/n3"); // NOPMD
				/* turtle is a subset of n3 for RDF */
				assertThat(n3).isNotEmpty();
				assertThat(n3).isNotEmpty().isEqualTo(turtle);
			}
		});
	}

	@Test
	public void searchViaApiWithContentNegotiationJson() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				assertJsonResponse(call(ENDPOINT, "application/json"));
			}
		});
	}

	@Test
	public void searchViaApiWithContentNegotiationDefault() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				assertJsonResponse(call(ENDPOINT, "*/*"));
			}
		});
	}

	@Test
	public void searchViaApiWithContentNegotiationOverrideWithParam() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				assertJsonResponse(call(ENDPOINT + "&format=full", "text/html"));
			}
		});
	}

	private static void assertJsonResponse(final String response) {
		assertThat(response).isNotEmpty().startsWith("[{\"@").contains("@context")
				.contains("@graph").endsWith("}]");
	}

	@Test
	public void searchViaApiWithContentNegotiationBrowser() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				assertThat(
						call(ENDPOINT,
								"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"))
						.isNotEmpty().contains("<html");
			}
		});
	}

	@Test
	public void searchWithLimit() {
		final Index index = Index.LOBID_RESOURCES;
		final Parameter parameter = Parameter.AUTHOR;
		assertThat(
				new Search("Abr", index, parameter).page(0, 3).documents().size())
				.isEqualTo(3);
		assertThat(
				new Search("Abr", index, parameter).page(3, 6).documents().size())
				.isEqualTo(6);
	}

	@Test(expected = IllegalArgumentException.class)
	public void searchWithLimitInvalidFrom() {
		new Search("ha", Index.LOBID_RESOURCES, Parameter.AUTHOR).page(-1, 3)
				.documents();
	}

	@Test(expected = IllegalArgumentException.class)
	public void searchWithLimitInvalidSize() {
		new Search("ha", Index.LOBID_RESOURCES, Parameter.AUTHOR).page(0, 101)
				.documents();
	}

	@Test
	public void searchWithLimitApi() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				assertThat(call("resource?author=ha&from=0&size=3")).isNotEmpty()
						.isNotEqualTo(call("resource?author=ha&from=3&size=6"));
			}
		});
	}

	@Test
	public void searchWithLimitApiDefaults() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				assertThat(call("resource?author=ha&from=0&size=3")).isEqualTo(
						call("resource?author=ha&size=3")); /* default 'from' is 0 */
				assertThat(call("resource?author=ha&from=10&size=50")).isEqualTo(
						call("resource?author=ha&from=10")); /* default 'size' is 50 */
			}
		});
	}

	@Test
	public void testIdAndPrimaryTopicForResource() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final JsonNode jsonObject = Json.parse(call("resource?id=BT000001260"));
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.get(0).get("@id").asText()).isEqualTo(
						"http://lobid.org/resource/BT000001260/about");
				assertThat(jsonObject.get(0).get("primaryTopic").asText()).isEqualTo(
						"http://lobid.org/resource/BT000001260");
			}
		});
	}

	@Test
	public void testIdAndPrimaryTopicForPerson() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final JsonNode jsonObject = Json.parse(call("person?id=1019737174"));
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.get(0).get("@id").asText()).isEqualTo(
						"http://d-nb.info/gnd/1019737174/about");
				assertThat(jsonObject.get(0).get("primaryTopic").asText()).isEqualTo(
						"http://d-nb.info/gnd/1019737174");
			}
		});
	}
}
