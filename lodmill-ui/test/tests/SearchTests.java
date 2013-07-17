/* Copyright 2012-2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package tests;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.fest.assertions.Assertions.assertThat;
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

import org.codehaus.jackson.JsonNode;
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
		File sampleData = new File("test/tests/json-ld-index-data");
		try (Scanner scanner = new Scanner(sampleData)) {
			List<BulkItemResponse> runBulkRequests =
					IndexFromHdfsInElasticSearch.runBulkRequests(scanner, client);
			for (BulkItemResponse bulkItemResponse : runBulkRequests) {
				System.out.println(bulkItemResponse.toString());
			}
		}
		Thread.sleep(1000);
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
				.isEqualTo(35);
		JsonNode json =
				Json.parse(client
						.prepareGet(Index.LOBID_RESOURCES.id(), "json-ld-lobid",
								"http://lobid.org/resource/BT000001260").execute().actionGet()
						.getSourceAsString());
		assertThat(json.isObject()).isTrue();
		assertThat(
				json.get("http://purl.org/dc/elements/1.1/creator#dateOfBirth")
						.toString()).isEqualTo("\"1906\"");
	}

	@Test
	public void searchViaModel() {
		final List<Document> docs =
				Search.documents(TERM, Index.LOBID_RESOURCES, Parameter.AUTHOR);
		assertThat(docs.size()).isPositive();
		for (Document document : docs) {
			assertThat(document.getMatchedField().toLowerCase()).contains(TERM);
		}
	}

	@Test
	public void searchViaModelOrgName() {
		final String term = "hbz Land";
		final List<Document> docs =
				Search.documents(term, Index.LOBID_ORGANISATIONS, Parameter.NAME);
		assertThat(docs.size()).isEqualTo(1);
	}

	/*@formatter:off*/
	@Test public void searchViaModelOrgIdShort() { searchOrgById("DE-605"); }
	@Test public void searchViaModelOrgIdLong() { searchOrgById("http://lobid.org/organisation/DE-605"); }
	/*@formatter:on*/

	private static void searchOrgById(final String term) {
		final List<Document> docs =
				Search.documents(term, Index.LOBID_ORGANISATIONS, Parameter.ID);
		assertThat(docs.size()).isEqualTo(1);
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
				Search.documents(name, Index.LOBID_RESOURCES, Parameter.AUTHOR).size())
				.isEqualTo(1);
	}

	@Test
	public void searchViaModelMultiResult() {
		List<Document> documents =
				Search.documents("Neil Eric Schore (1948-)", Index.LOBID_RESOURCES,
						Parameter.AUTHOR);
		assertThat(documents.size()).isEqualTo(1);
		assertThat(documents.get(0).getMatchedField()).isEqualTo(
				"[Vollhardt, Kurt Peter C., Schore, Neil Eric] ([1948, 1946]-)");
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
	public void searchViaApiPage() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				assertThat(call("resource?author=abraham&format=page")).contains(
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
				assertThat(jsonObject.getElements().next().isContainerNode()).isTrue();
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
				assertThat(jsonObject.getElements().next().isContainerNode()).isFalse();
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
		Iterator<JsonNode> elements = jsonObject.getElements();
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
	@Test public void searchPrefNameNgram(){ searchName("Hanne", 3); }
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
				String gndId = "171932048";
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

	@Test
	public void searchViaApiWithContentNegotiation() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				String endpoint = "resource?author=abraham";
				final String nTriples = call(endpoint, "text/plain");
				final String turtle = call(endpoint, "text/turtle");
				final String rdfa = call(endpoint, "text/html");
				final String n3 = call(endpoint, "text/n3"); // NOPMD
				assertThat(nTriples).isNotEmpty().isNotEqualTo(turtle);
				assertThat(rdfa).isNotEmpty().contains("<html>");
				/* turtle is a subset of n3 for RDF */
				assertThat(turtle).isNotEmpty().isEqualTo(n3);
				assertThat(n3).isNotEmpty();
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
