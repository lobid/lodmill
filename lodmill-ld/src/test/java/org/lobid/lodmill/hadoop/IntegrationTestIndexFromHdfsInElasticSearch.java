/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Test the {@link IndexFromHdfsInElasticSearch} class.
 * <p/>
 * Uses mock in-memory HDFS and ElasticSearch instances.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public class IntegrationTestIndexFromHdfsInElasticSearch extends
		ClusterMapReduceTestCase {
	private static final int INDEX_SLEEP = 2000;
	private static final int DOC_COUNT = 22;
	private static final String TEST_FILE =
			"src/test/resources/json-ld-sample-output.json";
	private static final String DATA_1 = "json-es-test/part-r-00000";
	private static final String DATA_2 = "json-es-test/part-r-00001";
	private FileSystem hdfs = null;
	private static IndexFromHdfsInElasticSearch indexer = null;
	private static Client client;
	private static Node node;

	@Before
	@Override
	public void setUp() throws Exception {
		System.setProperty("hadoop.log.dir", "/tmp/logs");
		super.setUp();
		hdfs = getFileSystem();
		final Path srcPath = new Path(TEST_FILE);
		hdfs.copyFromLocalFile(srcPath, new Path(DATA_1));
		hdfs.copyFromLocalFile(srcPath, new Path(DATA_2));
		node = nodeBuilder().local(true).node();
		client = node.client();
		indexer = new IndexFromHdfsInElasticSearch(hdfs, client);
	}

	@Test
	public void testIndexOne() throws IOException, InterruptedException {
		assertNotNull("Indexer should have been created", indexer);
		final List<BulkItemResponse> errors = indexer.indexOne(DATA_1);
		for (BulkItemResponse error : errors) {
			System.err.println("Index error: " + error.getFailureMessage());
		}
		Thread.sleep(INDEX_SLEEP);
		assertEquals("All documents should be indexed", DOC_COUNT, client
				.prepareSearch().execute().actionGet().getHits().totalHits());
		assertEquals("Indexing one should yield no errors", 0, errors.size());
	}

	@Test
	public void testIndexAll() throws IOException, InterruptedException {
		final List<BulkItemResponse> errors = indexer.indexAll("json-es-test/", "");
		for (BulkItemResponse error : errors) {
			System.err.println("Index error: " + error.getFailureMessage());
		}
		Thread.sleep(INDEX_SLEEP);
		assertEquals("All documents should be indexed", DOC_COUNT, client
				.prepareSearch().execute().actionGet().getHits().totalHits());
		assertEquals("Indexing all should yield no errors", 0, errors.size());
	}

	@Test
	public void testNGram() throws IOException, InterruptedException {
		indexer.indexAll("json-es-test/", "");
		Thread.sleep(INDEX_SLEEP);
		final SearchResponse response =
				search(
						"lobid-index",
						"@graph.http://d-nb.info/standards/elementset/gnd#preferredNameForThePerson.@value",
						"loft");
		assertTrue(
				"Substring of actual index term should yield results due to ngram config",
				response.getHits().iterator().hasNext());
	}

	private static SearchResponse search(final String index, final String field,
			final String term) {
		final SearchResponse response =
				client.prepareSearch(index)
						.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
						.setQuery(QueryBuilders.termQuery(field, term)).setFrom(0)
						.setSize(10).setExplain(true).execute().actionGet();
		return response;
	}

	@Override
	@After
	public void tearDown() {
		client.admin().indices().prepareDelete().execute().actionGet();
		node.close();
		try {
			hdfs.close();
			super.stopCluster();
		} catch (Exception e) {
			LoggerFactory
					.getLogger(IntegrationTestIndexFromHdfsInElasticSearch.class).error(
							e.getMessage(), e);
		}
	}
}
