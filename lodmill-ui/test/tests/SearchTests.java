/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

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
import java.util.List;
import java.util.Scanner;

import models.Document;
import models.Search;
import models.Index;

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
				.isEqualTo(30);
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
				Search.documents(TERM, Index.LOBID_RESOURCES, "author");
		assertThat(docs.size()).isPositive();
		for (Document document : docs) {
			assertThat(document.getMatchedField().toLowerCase()).contains(TERM);
		}
	}

	@Test
	public void searchViaModelBirth() {
		assertThat(
				Search.documents("Hundt, Theo (1906-)", Index.LOBID_RESOURCES,
						"author").size()).isEqualTo(1);
	}

	@Test
	public void searchViaModelBirthDeath() {
		assertThat(
				Search.documents("Goeters, Johann F. Gerhard (1926-1996)",
						Index.LOBID_RESOURCES, "author").size()).isEqualTo(1);
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
				assertThat(
						call("search?index=" + Index.LOBID_RESOURCES.id()
								+ "&query=abraham&format=page&category=author")).contains(
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
						Json.parse(call("search?index=" + Index.LOBID_RESOURCES.id()
								+ "&query=abraham&format=full&category=author"));
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
						Json.parse(call("search?index=" + Index.LOBID_RESOURCES.id()
								+ "&query=abraham&format=short&category=author"));
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.size()).isGreaterThan(5).isLessThan(10);
				assertThat(jsonObject.getElements().next().isContainerNode()).isFalse();
			}
		});
	}

	@Test
	public void searchViaApiGnd() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final JsonNode jsonObject =
						Json.parse(call("search?index=" + Index.GND.id()
								+ "&query=bach&format=short&category=author"));
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
						Json.parse(call("search?index=" + Index.GND.id() + "&query="
								+ name.replace(" ", "%20") + "&format=short&category=author"));
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.size()).isEqualTo(results);
				if (results > 0) {
					assertThat(jsonObject.get(0).asText()).isEqualTo(
							"Schmidt, Hannelore (1919-2010)");
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
				final JsonNode jsonObject = Json.parse(call("author/" + gndId));
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
				final JsonNode jsonObject = Json.parse(call("keyword/" + gndId));
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
				final String nTriples = call("author/abraham", "text/plain");
				final String turtle = call("author/abraham", "text/turtle");
				final String rdfa = call("author/abraham", "text/html");
				final String n3 = call("author/abraham", "text/n3"); // NOPMD
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
