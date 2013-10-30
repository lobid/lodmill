/* Copyright 2012-2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package tests;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.fest.assertions.Assertions.assertThat;
import static play.mvc.Http.Status.BAD_REQUEST;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.GET;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.fakeRequest;
import static play.test.Helpers.route;
import static play.test.Helpers.running;
import static play.test.Helpers.status;
import static play.test.Helpers.testServer;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import models.Document;
import models.Index;
import models.Parameter;
import models.Search;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lobid.lodmill.hadoop.IndexFromHdfsInElasticSearch;

import play.libs.Json;
import play.mvc.Result;
import play.test.TestServer;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;

/**
 * Tests for the search functionality.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public class SearchTests {

	private static final int FROM = 0;
	private static final int SIZE = 50;
	private static final Index TEST_INDEX = Index.LOBID_RESOURCES;
	static final String TERM = "theo";
	static final int TEST_SERVER_PORT = 5000;
	static final TestServer TEST_SERVER = testServer(TEST_SERVER_PORT);
	private static Node node;
	private static Client client;

	@BeforeClass
	public static void setup() throws IOException, InterruptedException {
		node = nodeBuilder().local(true).node();
		client = node.client();
		client.admin().indices().prepareDelete().execute().actionGet();
		File sampleData = new File("test/tests/json-ld-index-data.json");
		try (Scanner scanner = new Scanner(sampleData)) {
			List<BulkItemResponse> runBulkRequests =
					IndexFromHdfsInElasticSearch.runBulkRequests(scanner, client);
			for (BulkItemResponse bulkItemResponse : runBulkRequests) {
				System.out.println(bulkItemResponse.toString());
			}
		}
		Thread.sleep(2000);
		Search.clientSet(client);
	}

	@AfterClass
	public static void down() {
		client.admin().indices().prepareDelete(TEST_INDEX.id()).execute()
				.actionGet();
		node.close();
		Search.clientReset();
	}

	@Test
	public void accessIndex() {
		assertThat(
				client.prepareSearch().execute().actionGet().getHits().totalHits())
				.isEqualTo(41);
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
				Search.documents(TERM, Index.LOBID_RESOURCES, Parameter.AUTHOR, FROM,
						SIZE, "");
		assertThat(docs.size()).isPositive();
		for (Document document : docs) {
			assertThat(document.getMatchedField().toLowerCase()).contains(TERM);
		}
	}

	@Test
	public void searchViaModelOrgName() {
		assertThat(searchOrgByName("hbz Land")).isEqualTo(1);
		assertThat(searchOrgByName("hbz Schmeckermeck")).isEqualTo(0);
	}

	private static int searchOrgByName(final String term) {
		return Search.documents(term, Index.LOBID_ORGANISATIONS, Parameter.NAME,
				FROM, SIZE, "").size();
	}

	@Test
	public void searchViaModelOrgQuery() {
		assertThat(searchOrgQuery("Einrichtung ohne Bestand")).isEqualTo(1);
		assertThat(searchOrgQuery("hbz Schmeckermeck")).isEqualTo(1);
	}

	private static int searchOrgQuery(final String term) {
		return Search.documents(term, Index.LOBID_ORGANISATIONS, Parameter.Q, FROM,
				SIZE, "").size();
	}

	/*@formatter:off*/
	@Test public void searchViaModelOrgIdShort() { searchOrgById("DE-605"); }
	@Test public void searchViaModelOrgIdLong() { searchOrgById("http://lobid.org/organisation/DE-605"); }
	/*@formatter:on*/

	private static void searchOrgById(final String term) {
		final List<Document> docs =
				Search.documents(term, Index.LOBID_ORGANISATIONS, Parameter.ID, FROM,
						SIZE, "");
		assertThat(docs.size()).isEqualTo(1);
	}

	/*@formatter:off*/
	@Test public void searchResByIdTT() { searchResById("TT002234003"); }
	@Test public void searchResByIdHT() { searchResById("HT002189125"); }
	@Test public void searchResByIdZDB() { searchResById("ZDB2615620-9"); }
	@Test public void searchResByIdTTUrl() { searchResById("http://lobid.org/resource/TT002234003"); }
	@Test public void searchResByIdHTUrl() { searchResById("http://lobid.org/resource/HT002189125"); }
	@Test public void searchResByIdZDBUrl() { searchResById("http://lobid.org/resource/ZDB2615620-9"); }
	@Test public void searchResByIdISBN() { searchResById("0940450003"); }
	@Test public void searchResByIdUrn() { searchResById("urn:nbn:de:101:1-201210094953"); }
	/*@formatter:on*/

	private static void searchResById(final String term) {
		final List<Document> docs =
				Search.documents(term, Index.LOBID_RESOURCES, Parameter.ID, FROM, SIZE,
						"");
		assertThat(docs.size()).isEqualTo(1);
	}

	@Test
	public void searchResByIdWithReturnFieldViaModel() {
		running(fakeApplication(), new Runnable() {
			@Override
			public void run() {
				final List<Document> docs =
						Search.documents("TT050326640", Index.LOBID_RESOURCES,
								Parameter.ID, FROM, SIZE, "fulltextOnline");
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
				Search.documents(name, Index.LOBID_RESOURCES, Parameter.AUTHOR, FROM,
						SIZE, "").size()).isEqualTo(1);
	}

	@Test
	public void searchViaModelMultiResult() {
		List<Document> documents =
				Search.documents("Neil Eric Schore (1948-)", Index.LOBID_RESOURCES,
						Parameter.AUTHOR, FROM, SIZE, "");
		assertThat(documents.size()).isEqualTo(1);
		assertThat(documents.get(0).getMatchedField()).isEqualTo(
				"Vollhardt, Kurt Peter C. (1946-)");
	}

	@Test
	public void searchViaModelSetNwBib() {
		List<Document> documents =
				Search.documents("NwBib", Index.LOBID_RESOURCES, Parameter.SET, FROM,
						SIZE, "");
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
				assertThat(call("")).contains("<html>");
			}
		});
	}

	@Test
	public void searchViaApiHtml() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				assertThat(call("resource?author=abraham", "text/html")).contains(
						"<html>");
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
				assertThat(sorted(list(jsonObject))).isEqualTo(list(jsonObject));
				assertThat(jsonObject.size()).isGreaterThan(5).isLessThan(10);
				assertThat(jsonObject.elements().next().isContainerNode()).isFalse();
			}

			private List<String> sorted(List<String> list) {
				List<String> sorted = new ArrayList<>(list);
				Collections.sort(sorted);
				return sorted;
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
						Json.parse(call("person?name=bach&format=short"));
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.size()).isEqualTo(5); /* differentiated only */
			}
		});
	}

	/* @formatter:off */
	@Test public void searchAltNamePlain() { searchName("Schmidt, Loki", 1); }
	@Test public void searchAltNameSwap()  { searchName("Loki Schmidt", 1); }
	@Test public void searchAltNameSecond(){ searchName("Hannelore Glaser", 1); }
	@Test public void searchAltNameShort() { searchName("Loki", 1); }
	@Test public void searchAltNameNgram() { searchName("Lok", 1); }
	@Test public void searchPrefNameNgram(){ searchName("Hanne", 2); }
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
	@Test public void resourceByGndSubjectMulti(){gndSubject("44141956", 2);}
	@Test public void resourceByGndSubjectDashed(){gndSubject("4414195-6", 1);}
	@Test public void resourceByGndSubjectSingle(){gndSubject("189452846", 1);}
	/* @formatter:on */

	public void gndSubject(final String gndId, final int results) {
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
	@Test public void itemByIdParam(){findItem("item?id=BT000000079%3AGA+644");}
	@Test public void itemByIdRoute(){findItem("item/BT000000079%3AGA+644");}
	@Test public void itemByIdUri(){findItem("item?id=http://lobid.org/item/BT000000079%3AGA+644");}
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
				assertThat(call(ENDPOINT, "text/plain")).isNotEmpty().startsWith(
						"<http");
			}
		});
	}

	@Test
	public void searchViaApiWithContentNegotiationTurtle() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				assertThat(call(ENDPOINT, "text/turtle")).isNotEmpty().contains(
						"      a       ");
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
		assertThat(response).isNotEmpty().startsWith("[{\"@context\":");
	}

	@Test
	public void searchViaApiWithContentNegotiationBrowser() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				assertThat(
						call(ENDPOINT,
								"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"))
						.isNotEmpty().contains("<html>");
			}
		});
	}

	@Test
	public void searchWithLimit() {
		final Index index = Index.LOBID_RESOURCES;
		final Parameter parameter = Parameter.AUTHOR;
		assertThat(Search.documents("ha", index, parameter, 0, 3, "").size())
				.isEqualTo(3);
		assertThat(Search.documents("ha", index, parameter, 3, 6, "").size())
				.isEqualTo(6);
	}

	@Test(expected = IllegalArgumentException.class)
	public void searchWithLimitInvalidFrom() {
		Search.documents("ha", Index.LOBID_RESOURCES, Parameter.AUTHOR, -1, 3, "");
	}

	@Test(expected = IllegalArgumentException.class)
	public void searchWithLimitInvalidSize() {
		Search.documents("ha", Index.LOBID_RESOURCES, Parameter.AUTHOR, 0, 101, "");
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

	static String call(final String request) {
		return call(request, "application/json");
	}

	private static String call(final String request, final String contentType) {
		try {
			final URLConnection url =
					new URL("http://localhost:" + TEST_SERVER_PORT + "/" + request)
							.openConnection();
			url.setRequestProperty("Accept", contentType);
			return CharStreams.toString(new InputStreamReader(url.getInputStream(),
					Charsets.UTF_8));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
}
