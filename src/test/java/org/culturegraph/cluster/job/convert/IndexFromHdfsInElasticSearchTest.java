package org.culturegraph.cluster.job.convert;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.json.JSONException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the {@link IndexFromHdfsInElasticSearch} class.
 * <p/>
 * Requires local HDFS and ElasticSearch instances (set constants below).
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class IndexFromHdfsInElasticSearchTest {
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

	@BeforeClass
	public static void upload() throws IOException {
		hdfs = FileSystem.get(URI.create(HDFS_SERVER), new Configuration());
		final Path srcPath = new Path(TEST_FILE);
		hdfs.copyFromLocalFile(srcPath, new Path(DATA_1));
		hdfs.copyFromLocalFile(srcPath, new Path(DATA_2));
		final Client client =
				new TransportClient(ImmutableSettings.settingsBuilder()
						.put("cluster.name", ES_CLUSTER_NAME).build())
						.addTransportAddress(ES_SERVER);
		indexer = new IndexFromHdfsInElasticSearch(hdfs, client);
	}

	@Test
	public void testIndexOne() throws IOException, JSONException,
			InterruptedException {
		Assert.assertEquals(25, indexer.indexOne(DATA_1).size());
	}

	@Test
	public void testIndexAll() throws IOException, JSONException,
			InterruptedException {
		Assert.assertEquals(50, indexer.indexAll("json-es-test/").size());
	}

	@AfterClass
	public static void close() throws IOException {
		hdfs.close();
	}
}
