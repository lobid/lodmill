/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
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
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	public IndexFromHdfsInElasticSearch(final FileSystem hdfs,
			final Client client) {
		this.hdfs = hdfs;
		this.client = client;
	}

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
				new TransportClient(ImmutableSettings
						.settingsBuilder()
						.put("cluster.name", args[3])
						.put("client.transport.sniff", false)
						.put("client.transport.ping_timeout", 20,
								TimeUnit.SECONDS).build())
						.addTransportAddress(new InetSocketTransportAddress(
								args[2], 9300));
		final IndexFromHdfsInElasticSearch indexer =
				new IndexFromHdfsInElasticSearch(hdfs, client);
		indexer.indexAll(args[1].endsWith("/") ? args[1] : args[1] + "/");
	}

	public List<BulkItemResponse> indexAll(final String dir) throws IOException {
		checkPathInHdfs(dir);
		final List<BulkItemResponse> result = new ArrayList<BulkItemResponse>();
		final FileStatus[] listStatus = hdfs.listStatus(new Path(dir));
		for (FileStatus fileStatus : listStatus) {
			LOG.info("Indexing: " + fileStatus.getPath().getName());
			if (fileStatus.getPath().getName().startsWith("part-")) {
				result.addAll(indexOne(dir + fileStatus.getPath().getName()));
			}
		}
		return result;
	}

	public List<BulkItemResponse> indexOne(final String data)
			throws IOException {
		checkPathInHdfs(data);
		final FSDataInputStream inputStream = hdfs.open(new Path(data));
		final Scanner scanner = new Scanner(inputStream, "UTF-8");
		final List<BulkRequestBuilder> bulkRequests =
				createBulkRequests(scanner);
		scanner.close();
		final List<BulkItemResponse> result = new ArrayList<BulkItemResponse>();
		for (BulkRequestBuilder bulkRequest : bulkRequests) {
			final BulkResponse bulkResponse = executeBulkRequest(bulkRequest);
			if (bulkResponse == null) {
				LOG.error("Bulk request failed for " + data);
				return Collections.emptyList();
			} else {
				collectSuccessfulResponses(result, bulkResponse);
			}
		}
		return result;
	}

	private void collectSuccessfulResponses(
			final List<BulkItemResponse> result, final BulkResponse bulkResponse) {
		for (BulkItemResponse bulkItemResponse : bulkResponse) {
			if (bulkItemResponse.failed()) {
				LOG.error(bulkItemResponse.failureMessage());
			} else {
				result.add(bulkItemResponse);
			}
		}
	}

	private List<BulkRequestBuilder> createBulkRequests(final Scanner scanner) {
		int lineNumber = 0;
		String meta = null;
		final List<BulkRequestBuilder> bulks =
				new ArrayList<BulkRequestBuilder>();
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		while (scanner.hasNextLine()) {
			final String line = scanner.nextLine();
			if (lineNumber % 2 == 0) { // every first line is index info
				meta = line;
			} else { // every second line is value object
				@SuppressWarnings("unchecked")
				final Map<String, Object> map =
						(Map<String, Object>) JSONValue.parse(line);
				final IndexRequestBuilder requestBuilder =
						createRequestBuilder(meta, map);
				bulkRequest.add(requestBuilder);
				LOG.debug("Bulk index request:" + requestBuilder);
			}
			lineNumber++;
			/* Split into multiple bulks to ease server load: */
			if (lineNumber % BULK_SIZE == 0) {
				bulks.add(bulkRequest);
				bulkRequest = client.prepareBulk();
			}
		}
		/* Add the final bulk, if there is anything to do: */
		if (bulkRequest.numberOfActions() > 0) {
			bulks.add(bulkRequest);
		}
		return bulks;
	}

	private IndexRequestBuilder createRequestBuilder(final String meta,
			final Map<String, Object> map) {
		final JSONObject object =
				(JSONObject) ((JSONObject) JSONValue.parse(meta)).get("index");
		final String index = (String) object.get("_index");
		final String type = (String) object.get("_type");
		final String id = (String) object.get("_id"); // NOPMD
		return client.prepareIndex(index, type, id).setSource(map);
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
				LOG.error(String
						.format("Retry bulk index request after exception: %s (%s more retries)",
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
