/* Copyright 2014 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package tests;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static play.test.Helpers.testServer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import models.Index;
import models.Search;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.node.Node;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import play.test.TestServer;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

/**
 * Tests harness for the search tests. Creates an in-memory ES index.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public class SearchTestsHarness {

	private static final String TEST_CONFIG = "test/tests/index-config-test.json";
	private static final String TEST_DATA = "test/tests/json-ld-index-data.json";
	private static final Index TEST_INDEX = Index.LOBID_RESOURCES;
	static final int TEST_SERVER_PORT = 5000;
	static final TestServer TEST_SERVER = testServer(TEST_SERVER_PORT);
	private static Node node;
	protected static Client client;
	private static final Logger LOG = LoggerFactory
			.getLogger(SearchTestsHarness.class);

	@BeforeClass
	public static void setup() throws IOException {
		node = nodeBuilder().local(true).node();
		client = node.client();
		client.admin().indices().prepareDelete().execute().actionGet();
		client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute()
				.actionGet();
		File sampleData = new File(TEST_DATA);
		try (Scanner scanner = new Scanner(sampleData)) {
			List<BulkItemResponse> runBulkRequests = runBulkRequests(scanner, client);
			for (BulkItemResponse bulkItemResponse : runBulkRequests) {
				System.out.println(bulkItemResponse.toString());
			}
		}
		client.admin().indices().refresh(new RefreshRequest()).actionGet();
		Search.clientSet(client);
	}

	@AfterClass
	public static void down() {
		client.admin().indices().prepareDelete(TEST_INDEX.id()).execute()
				.actionGet();
		node.close();
		Search.clientReset();
	}

	static String call(final String request) {
		return call(request.replace(' ', '+').replace("#", "%23"),
				"application/json");
	}

	static String call(final String request, final String contentType) {
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

	private static List<BulkItemResponse> runBulkRequests(final Scanner scanner,
			final Client c) {
		final List<BulkItemResponse> result = new ArrayList<>();
		int lineNumber = 0;
		String meta = null;
		BulkRequestBuilder bulkRequest = c.prepareBulk();
		while (scanner.hasNextLine()) {
			final String line = scanner.nextLine();
			if (lineNumber % 2 == 0)
				meta = line;
			else
				addIndexRequest(meta, bulkRequest, line, c);
			lineNumber++;
		}
		if (bulkRequest.numberOfActions() > 0)
			runBulkRequest(bulkRequest, result);
		return result;
	}

	private static void addIndexRequest(final String meta,
			final BulkRequestBuilder bulkRequest, final String line, final Client c) {
		try {
			final Map<String, Object> map =
					(Map<String, Object>) JSONValue.parseWithException(line);
			final IndexRequestBuilder requestBuilder =
					createRequestBuilder(meta, map, c);
			bulkRequest.add(requestBuilder);
		} catch (ParseException e) {
			LOG.error(String.format(
					"ParseException with meta '%s' and line '%s': '%s'", meta, line,
					e.getMessage()), e);
		}
	}

	private static void runBulkRequest(final BulkRequestBuilder bulkRequest,
			final List<BulkItemResponse> result) {
		final BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse == null) {
			LOG.error("Bulk request failed: " + bulkRequest);
		} else {
			collectFailedResponses(result, bulkResponse);
		}
	}

	private static void collectFailedResponses(
			final List<BulkItemResponse> result, final BulkResponse bulkResponse) {
		for (BulkItemResponse response : bulkResponse) {
			if (response.isFailed()) {
				LOG.error(String.format(
						"Bulk item response failed for index '%s', ID '%s', message: %s",
						response.getIndex(), response.getId(), response.getFailureMessage()));
				result.add(response);
			}
		}
	}

	private static IndexRequestBuilder createRequestBuilder(final String meta,
			final Map<String, Object> map, Client c) {
		final JSONObject object =
				(JSONObject) ((JSONObject) JSONValue.parse(meta)).get("index");
		final String index = (String) object.get("_index");
		final String type = (String) object.get("_type");
		final String id = (String) object.get("_id"); // NOPMD
		final String parent = (String) object.get("_parent");
		final IndicesAdminClient admin = c.admin().indices();
		if (!admin.prepareExists(index).execute().actionGet().isExists()) {
			admin.prepareCreate(index).setSource(config()).execute().actionGet();
		}
		final IndexRequestBuilder request =
				c.prepareIndex(index, type, id).setSource(map);
		return parent == null ? request : request.setParent(parent);
	}

	private static String config() {
		String res = null;
		try {
			final InputStream config = new FileInputStream(TEST_CONFIG);
			try (InputStreamReader reader = new InputStreamReader(config, "UTF-8")) {
				res = CharStreams.toString(reader);
			}
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return res;
	}
}
