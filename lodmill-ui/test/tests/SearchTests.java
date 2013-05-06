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

	private static final String TEST_INDEX = "lobid-index";
	static final String TERM = "theo";
	static final int TEST_SERVER_PORT = 5000;
	static final TestServer TEST_SERVER = testServer(TEST_SERVER_PORT);
	private static Node node;
	private static Client client;

	@BeforeClass
	public static void setup() throws IOException, InterruptedException {
		node = nodeBuilder().local(true).node();
		client = node.client();
		File sampleData = new File("test/tests/json-ld-index-data");
		try (Scanner scanner = new Scanner(sampleData)) {
			List<BulkItemResponse> runBulkRequests =
					IndexFromHdfsInElasticSearch.runBulkRequests(scanner, client);
			for (BulkItemResponse bulkItemResponse : runBulkRequests) {
				System.out.println(bulkItemResponse.toString());
			}
		}
		Thread.sleep(1000);
		Document.clientSet(client);
	}

	@AfterClass
	public static void down() {
		client.admin().indices().prepareDelete(TEST_INDEX).execute().actionGet();
		node.close();
		Document.clientReset();
	}

	@Test
	public void accessIndex() {
		assertThat(
				client.prepareSearch().execute().actionGet().getHits().totalHits())
				.isEqualTo(25);
		JsonNode json =
				Json.parse(client
						.prepareGet("lobid-index", "json-ld-lobid",
								"http://lobid.org/resource/BT000001260").execute().actionGet()
						.getSourceAsString());
		assertThat(json.isObject()).isTrue();
		assertThat(
				json.get("http://purl.org/dc/elements/1.1/creator#dateOfBirth")
						.toString()).isEqualTo("\"1906\"");
	}

	@Test
	public void searchViaModel() {
		final List<Document> docs = Document.search(TERM, "lobid-index", "author");
		assertThat(docs.size()).isPositive();
		for (Document document : docs) {
			assertThat(document.getMatchedField().toLowerCase()).contains(TERM);
		}
	}

	@Test
	public void searchViaModelBirth() {
		assertThat(
				Document.search("Hundt, Theo (1906-)", "lobid-index", "author").size())
				.isEqualTo(1);
	}

	@Test
	public void searchViaModelBirthDeath() {
		assertThat(
				Document.search("Goeters, Johann F. Gerhard (1926-1996)",
						"lobid-index", "author").size()).isEqualTo(1);
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
						call("search?index=lobid-index&query=abraham&format=page&category=author"))
						.contains("<html>");
			}
		});
	}

	@Test
	public void searchViaApiFull() {
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				final JsonNode jsonObject =
						Json.parse(call("search?index=lobid-index&query=abraham&format=full&category=author"));
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
						Json.parse(call("search?index=lobid-index&query=abraham&format=short&category=author"));
				assertThat(jsonObject.isArray()).isTrue();
				assertThat(jsonObject.size()).isGreaterThan(5).isLessThan(10);
				assertThat(jsonObject.getElements().next().isContainerNode()).isFalse();
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
