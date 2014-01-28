/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import static java.lang.String.format;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
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

	/**
	 * @param args Pass 4 params: hdfs-server hdfs-input-path es-host
	 *          es-cluster-name to index files in hdfs-input-path from HDFS on
	 *          hdfs-server into es-cluster-name on es-host. Optional 5. argument:
	 *          a suffix to append to the index aliases created (e.g. `-staging`).
	 *          If the 5. argument is 'NOALIAS', alias creation is skipped"
	 */
	public static void main(final String[] args) {
		if (args.length < 4 || args.length > 5) {
			System.err
					.println("Pass 4 params: <hdfs-server> <hdfs-input-path>"
							+ " <es-host> <es-cluster-name> to index files in"
							+ " <hdfs-input-path> from HDFS on <hdfs-server> into"
							+ " <es-cluster-name> on <es-host>. Optional 5. argument:"
							+ "a suffix to append to the index aliases created (e.g. `-staging`)."
							+ " If the 5. argument is 'NOALIAS', alias creation is skipped");
			System.exit(-1);
		}
		try (FileSystem hdfs =
				FileSystem.get(URI.create(args[0]), new Configuration())) {
			final Client client =
					new TransportClient(ImmutableSettings.settingsBuilder()
							.put("cluster.name", args[3])
							.put("client.transport.sniff", false)
							.put("client.transport.ping_timeout", 20, TimeUnit.SECONDS)
							.build()).addTransportAddress(new InetSocketTransportAddress(
							args[2], 9300));
			final IndexFromHdfsInElasticSearch indexer =
					new IndexFromHdfsInElasticSearch(hdfs, client);
			indexer.indexAll(args[1].endsWith("/") ? args[1] : args[1] + "/",
					args.length == 5 ? args[4] : "");
		} catch (IOException e) {
			e.printStackTrace();
		}
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
	 * @param aliasSuffix A suffix to append to the index aliases created
	 * @return A list of responses for requests that failed
	 * @throws IOException When HDFS operations fail
	 */
	public List<BulkItemResponse> indexAll(final String dir,
			final String aliasSuffix) throws IOException {
		checkPathInHdfs(dir);
		final List<BulkItemResponse> result = new ArrayList<>();
		final FileStatus[] listStatus = hdfs.listStatus(new Path(dir));
		for (FileStatus fileStatus : listStatus) {
			LOG.info("Indexing: " + fileStatus.getPath().getName());
			if (fileStatus.getPath().getName().startsWith("part-")) {
				result.addAll(indexOne(dir + fileStatus.getPath().getName()));
			}
		}
		if (!aliasSuffix.equals("NOALIAS"))
			updateAliases(aliasSuffix);
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
		List<BulkItemResponse> result = null;
		int retries = 40;
		while (retries > 0) {
			try (FSDataInputStream inputStream = hdfs.open(new Path(data));
					Scanner scanner = new Scanner(inputStream, "UTF-8")) {
				result = runBulkRequests(scanner, client);
				break; // stop retry-while
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
						"Retry indexing data at %s after exception: %s (%s more retries)",
						data, e.getMessage(), retries));
			}
		}
		return result;
	}

	/**
	 * @param scanner The scanner for reading the data to index
	 * @param c The elasticsearch client to use for indexing
	 * @return A list responses for requests that errored
	 */
	public static List<BulkItemResponse> runBulkRequests(final Scanner scanner,
			Client c) {
		final List<BulkItemResponse> result = new ArrayList<>();
		int lineNumber = 0;
		String meta = null;
		BulkRequestBuilder bulkRequest = c.prepareBulk();
		while (scanner.hasNextLine()) {
			final String line = scanner.nextLine();
			if (lineNumber % 2 == 0) { // every first line is index info
				meta = line;
			} else { // every second line is value object
				addIndexRequest(meta, bulkRequest, line, c);
			}
			lineNumber++;
			/* Split into multiple bulks to ease server load: */
			if (lineNumber % BULK_SIZE == 0) {
				runBulkRequest(bulkRequest, result);
				bulkRequest = c.prepareBulk();
			}
		}
		/* Run the final bulk, if there is anything to do: */
		if (bulkRequest.numberOfActions() > 0) {
			runBulkRequest(bulkRequest, result);
		}
		return result;
	}

	private void updateAliases(final String aliasSuffix) {
		final SortedSetMultimap<String, String> indices = groupByIndexCollection();
		for (String prefix : indices.keySet()) {
			final SortedSet<String> indicesForPrefix = indices.get(prefix);
			final String newIndex = indicesForPrefix.last();
			final String newAlias = prefix + aliasSuffix;
			LOG.info(format("Prefix '%s', newest index: %s", prefix, newIndex));
			removeOldAliases(indicesForPrefix, newAlias);
			createNewAlias(newIndex, newAlias);
			deleteOldIndices(indicesForPrefix);
		}
	}

	private SortedSetMultimap<String, String> groupByIndexCollection() {
		final SortedSetMultimap<String, String> indices = TreeMultimap.create();
		for (String index : client.admin().indices().prepareStatus().execute()
				.actionGet().getIndices().keySet()) {
			final String[] nameAndTimestamp = index.split("-(?=\\d)");
			indices.put(nameAndTimestamp[0], index);
		}
		return indices;
	}

	private void removeOldAliases(final SortedSet<String> indicesForPrefix,
			final String newAlias) {
		for (String indexName : indicesForPrefix) {
			final Set<String> aliases = aliases(indexName);
			for (String alias : aliases) {
				if (alias.equals(newAlias)) {
					LOG.info(format("Delete alias index,alias: %s,%s", indexName, alias));
					client.admin().indices().prepareAliases()
							.removeAlias(indexName, alias).execute().actionGet();
				}
			}
		}
	}

	private void createNewAlias(final String newIndex, final String newAlias) {
		LOG.info(format("Create alias index,alias: %s,%s", newIndex, newAlias));
		client.admin().indices().prepareAliases().addAlias(newIndex, newAlias)
				.execute().actionGet();
	}

	private void deleteOldIndices(final SortedSet<String> allIndices) {
		if (allIndices.size() >= 3) {
			final List<String> list = new ArrayList<>(allIndices);
			for (String indexToDelete : list.subList(0, list.size() - 2)) {
				if (aliases(indexToDelete).isEmpty()) {
					LOG.info(format("Deleting index: " + indexToDelete));
					client.admin().indices()
							.delete(new DeleteIndexRequest(indexToDelete)).actionGet();
				}
			}
		}
	}

	private Set<String> aliases(final String indexName) {
		final ClusterStateRequest clusterStateRequest =
				Requests.clusterStateRequest().filterRoutingTable(true)
						.filterNodes(true).filteredIndices(indexName);
		return client.admin().cluster().state(clusterStateRequest).actionGet()
				.getState().getMetaData().aliases().keySet();
	}

	private static void addIndexRequest(final String meta,
			final BulkRequestBuilder bulkRequest, final String line, Client c) {
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
			final InputStream config =
					Thread.currentThread().getContextClassLoader()
							.getResourceAsStream("index-config.json");
			try (InputStreamReader reader = new InputStreamReader(config, "UTF-8")) {
				res = CharStreams.toString(reader);
			}
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return res;
	}

	private void checkPathInHdfs(final String data) throws IOException {
		if (!hdfs.exists(new Path(data))) {
			throw new IllegalArgumentException("No such path in HDFS: " + data);
		}
	}

}
