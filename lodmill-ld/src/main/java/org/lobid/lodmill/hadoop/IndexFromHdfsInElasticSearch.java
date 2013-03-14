/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.CharStreams;

/**
 * Index JSON-LD Hadoop output in HDFS into an ElasticSearch instance.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class IndexFromHdfsInElasticSearch {

	private static final int BULK_SIZE = 1000;
	private static final Logger LOG = LoggerFactory
			.getLogger(IndexFromHdfsInElasticSearch.class);
	private final FileSystem hdfs;
	private final Client client;

	public static void main(final String[] args) throws IOException {
		if (args.length != 4) {
			System.err.println("Pass 4 params: <hdfs-server> <hdfs-input-path>"
					+ " <es-host> <es-cluster-name> to index files in"
					+ " <hdfs-input-path> from HDFS on <hdfs-server> into"
					+ " <es-cluster-name> on <es-host>.");
			System.exit(-1);
		}
		final FileSystem hdfs =
				FileSystem.get(URI.create(args[0]), new Configuration());
		final Client client =
				new TransportClient(ImmutableSettings.settingsBuilder()
						.put("cluster.name", args[3]).put("client.transport.sniff", false)
						.put("client.transport.ping_timeout", 20, TimeUnit.SECONDS).build())
						.addTransportAddress(new InetSocketTransportAddress(args[2], 9300));
		final IndexFromHdfsInElasticSearch indexer =
				new IndexFromHdfsInElasticSearch(hdfs, client);
		indexer.indexAll(args[1].endsWith("/") ? args[1] : args[1] + "/");
	}

	/**
	 * @param hdfs The HDFS to index from
	 * @param client The ElasticSearch client for indexing
	 */
	public IndexFromHdfsInElasticSearch(final FileSystem hdfs, final Client client) {
		this.hdfs = hdfs;
		this.client = client;
	}

	/**
	 * Index all data in the given directory
	 * 
	 * @param dir The directory to index
	 * @return A list of responses for requests that failed
	 * @throws IOException When HDFS operations fail
	 */
	public List<BulkItemResponse> indexAll(final String dir) throws IOException {
		checkPathInHdfs(dir);
		final List<BulkItemResponse> result = new ArrayList<>();
		final FileStatus[] listStatus = hdfs.listStatus(new Path(dir));
		for (FileStatus fileStatus : listStatus) {
			LOG.info("Indexing: " + fileStatus.getPath().getName());
			if (fileStatus.getPath().getName().startsWith("part-")) {
				result.addAll(indexOne(dir + fileStatus.getPath().getName()));
			}
		}
		return result;
	}

	/**
	 * Index data at the given location
	 * 
	 * @param data The data location
	 * @return A list of responses for requests that failed
	 * @throws IOException When HDFS operations fail
	 */
	public List<BulkItemResponse> indexOne(final String data) throws IOException {
		checkPathInHdfs(data);
		final FSDataInputStream inputStream = hdfs.open(new Path(data));
		final Scanner scanner = new Scanner(inputStream, "UTF-8");
		final List<BulkItemResponse> result = runBulkRequests(scanner);
		scanner.close();
		return result;
	}

	private List<BulkItemResponse> runBulkRequests(final Scanner scanner) {
		final List<BulkItemResponse> result = new ArrayList<>();
		int lineNumber = 0;
		String meta = null;
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		while (scanner.hasNextLine()) {
			final String line = scanner.nextLine();
			if (lineNumber % 2 == 0) { // every first line is index info
				meta = line;
			} else { // every second line is value object
				addIndexRequest(meta, bulkRequest, line);
			}
			lineNumber++;
			/* Split into multiple bulks to ease server load: */
			if (lineNumber % BULK_SIZE == 0) {
				runBulkRequest(bulkRequest, result);
				bulkRequest = client.prepareBulk();
			}
		}
		/* Run the final bulk, if there is anything to do: */
		if (bulkRequest.numberOfActions() > 0) {
			runBulkRequest(bulkRequest, result);
		}
		return result;
	}

	private void addIndexRequest(final String meta,
			final BulkRequestBuilder bulkRequest, final String line) {
		try {
			@SuppressWarnings("unchecked")
			final Map<String, Object> map =
					(Map<String, Object>) JSONValue.parseWithException(line);
			final IndexRequestBuilder requestBuilder =
					createRequestBuilder(meta, map);
			bulkRequest.add(requestBuilder);
		} catch (ParseException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	private void runBulkRequest(final BulkRequestBuilder bulkRequest,
			final List<BulkItemResponse> result) {
		final BulkResponse bulkResponse = executeBulkRequest(bulkRequest);
		if (bulkResponse == null) {
			LOG.error("Bulk request failed: " + bulkRequest);
		} else {
			collectFailedResponses(result, bulkResponse);
		}
	}

	private void collectFailedResponses(final List<BulkItemResponse> result,
			final BulkResponse bulkResponse) {
		for (BulkItemResponse bulkItemResponse : bulkResponse) {
			if (bulkItemResponse.failed()) {
				LOG.error(bulkItemResponse.failureMessage());
				result.add(bulkItemResponse);
			}
		}
	}

	private IndexRequestBuilder createRequestBuilder(final String meta,
			final Map<String, Object> map) {
		final JSONObject object =
				(JSONObject) ((JSONObject) JSONValue.parse(meta)).get("index");
		final String index = (String) object.get("_index");
		final String type = (String) object.get("_type");
		final String id = (String) object.get("_id"); // NOPMD
		final IndicesAdminClient admin = client.admin().indices();
		if (!admin.prepareExists(index).execute().actionGet().exists()) {
			admin.prepareCreate(index).setSource(config()).execute().actionGet();
		}
		return client.prepareIndex(index, type, id).setSource(map);
	}

	private String config() {
		String res = null;
		try {
			final InputStream config =
					Thread.currentThread().getContextClassLoader()
							.getResourceAsStream("index-config.json");
			res = CharStreams.toString(new InputStreamReader(config, "UTF-8"));
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return res;
	}

	private BulkResponse executeBulkRequest(final BulkRequestBuilder bulk) {
		BulkResponse bulkResponse = null;
		int retries = 40;
		while (retries > 0) {
			try {
				bulkResponse = bulk.execute().actionGet();
				break;
			} catch (NoNodeAvailableException e) {
				// Retry on NoNodeAvailableException, see
				// https://github.com/elasticsearch/elasticsearch/issues/1868
				retries--;
				try {
					Thread.sleep(10000);
				} catch (InterruptedException x) {
					LOG.error(x.getMessage(), x);
				}
				LOG.error(String.format(
						"Retry bulk index request after exception: %s (%s more retries)",
						e.getMessage(), retries));
			}
		}
		return bulkResponse;
	}

	private void checkPathInHdfs(final String data) throws IOException {
		if (!hdfs.exists(new Path(data))) {
			throw new IllegalArgumentException("No such path in HDFS: " + data);
		}
	}

}
