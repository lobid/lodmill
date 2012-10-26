package org.culturegraph.cluster.job.convert;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
		final BulkRequestBuilder bulkRequest = createBulkRequest(scanner);
		scanner.close();
		final BulkResponse bulkResponse = executeBulkRequest(bulkRequest);
		if (bulkResponse == null) {
			LOG.error("Bulk request failed for " + data);
			return Collections.emptyList();
		} else {
			if (bulkResponse.hasFailures()) {
				LOG.error("Bulk request errors for " + data);
				LOG.error(bulkResponse.buildFailureMessage());
			}
			return Arrays.asList(bulkResponse.items());
		}
	}

	private BulkRequestBuilder createBulkRequest(final Scanner scanner) {
		int lineNumber = 0;
		String index = null;
		String type = null;
		String id = null; // NOPMD (called 'id' as in ES API)
		final BulkRequestBuilder bulkRequest = client.prepareBulk();
		while (scanner.hasNextLine()) {
			final String line = scanner.nextLine();
			if (lineNumber % 2 == 0) { // every first line is index info
				final JSONObject object =
						(JSONObject) ((JSONObject) JSONValue.parse(line))
								.get("index");
				index = (String) object.get("_index");
				type = (String) object.get("_type");
				id = (String) object.get("_id");
			} else { // every second line is value object
				final IndexRequestBuilder requestBuilder =
						client.prepareIndex(index, type, id).setSource(
								line.getBytes(Charset.forName("UTF-8")));
				bulkRequest.add(requestBuilder);
				LOG.debug("Bulk index request:" + requestBuilder);
			}
			lineNumber++;
		}
		return bulkRequest;
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
