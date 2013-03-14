/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.json.JSONException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the {@link IndexFromHdfsInElasticSearch} class.
 * <p/>
 * Requires local HDFS and ElasticSearch instances (set constants below).
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class IndexFromHdfsInElasticSearchTests {
	private static final InetSocketTransportAddress ES_SERVER =
			new InetSocketTransportAddress("10.1.1.101", 9300); // NOPMD
	private static final String ES_CLUSTER_NAME = "es-lod-local";
	private static final String TEST_FILE =
			"src/test/resources/json-ld-sample-output";
	private static final String DATA_1 = "json-es-test/part-r-00000";
	private static final String DATA_2 = "json-es-test/part-r-00001";
	private static final String HDFS_SERVER = "hdfs://localhost:9000/";
	private static FileSystem hdfs = null;
	private static IndexFromHdfsInElasticSearch indexer = null;
	private static TransportClient client;

	@BeforeClass
	public static void upload() throws IOException {
		hdfs = FileSystem.get(URI.create(HDFS_SERVER), new Configuration());
		final Path srcPath = new Path(TEST_FILE);
		hdfs.copyFromLocalFile(srcPath, new Path(DATA_1));
		hdfs.copyFromLocalFile(srcPath, new Path(DATA_2));
		client =
				new TransportClient(ImmutableSettings.settingsBuilder()
						.put("cluster.name", ES_CLUSTER_NAME).build())
						.addTransportAddress(ES_SERVER);
		indexer = new IndexFromHdfsInElasticSearch(hdfs, client);
	}

	@Test
	public void testIndexOne() throws IOException, JSONException,
			InterruptedException {
		assertEquals("Indexing one should yield no errors", 0,
				indexer.indexOne(DATA_1).size());
	}

	@Test
	public void testIndexAll() throws IOException, JSONException,
			InterruptedException {
		assertEquals("Indexing all should yield no errors", 0,
				indexer.indexAll("json-es-test/").size());
	}

	@Test
	public void testNGram() throws IOException, JSONException,
			InterruptedException {
		indexer.indexAll("json-es-test/");
		Thread.sleep(200); // it seems the ngram analyzer needs a moment
		final SearchResponse response =
				search(
						"lobid-index",
						"http://purl.org/dc/elements/1.1/creator#preferredNameForThePerson",
						"loft");
		assertTrue(
				"Substring of actual index term should yield results due to ngram config",
				response.getHits().iterator().hasNext());
	}

	private SearchResponse search(final String index, final String field,
			final String term) {
		final SearchResponse response =
				client.prepareSearch(index)
						.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
						.setQuery(QueryBuilders.termQuery(field, term)).setFrom(0)
						.setSize(10).setExplain(true).execute().actionGet();
		return response;
	}

	@AfterClass
	public static void close() throws IOException {
		hdfs.close();
	}
}
