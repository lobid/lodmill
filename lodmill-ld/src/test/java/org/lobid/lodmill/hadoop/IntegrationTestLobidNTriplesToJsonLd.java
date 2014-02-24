/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lobid.lodmill.hadoop.NTriplesToJsonLd.NTriplesToJsonLdMapper;
import org.lobid.lodmill.hadoop.NTriplesToJsonLd.NTriplesToJsonLdReducer;
import org.slf4j.LoggerFactory;

/**
 * Test {@link #NTriplesToJsonLd} job with blank nodes.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public class IntegrationTestLobidNTriplesToJsonLd extends
		ClusterMapReduceTestCase {
	private static final String TEST_FILE_TRIPLES_1 =
			"src/test/resources/lobid-org-with-blank-nodes-1.nt";
	private static final String TEST_FILE_TRIPLES_2 =
			"src/test/resources/lobid-org-with-blank-nodes-2.nt";
	private static final String TEST_FILE_SUBJECTS =
			"src/test/resources/lobid-org-required-subjects.out";
	private static final String HDFS_IN_TRIPLES_1 =
			"blank-nodes-test/sample-1.nt";
	private static final String HDFS_IN_TRIPLES_2 =
			"blank-nodes-test/sample-2.nt";
	private static final String HDFS_IN_SUBJECTS = "blank-nodes-test/subjects";
	private static final String HDFS_OUT = "out/sample";
	private static final String HDFS_OUT_ZIP = "out/zip";
	private FileSystem hdfs = null;

	@Before
	@Override
	public void setUp() throws Exception {
		System.setProperty("hadoop.log.dir", "/tmp/logs");
		super.setUp();
		hdfs = getFileSystem();
		hdfs.copyFromLocalFile(new Path(TEST_FILE_TRIPLES_1), new Path(
				HDFS_IN_TRIPLES_1));
		hdfs.copyFromLocalFile(new Path(TEST_FILE_TRIPLES_2), new Path(
				HDFS_IN_TRIPLES_2));
		hdfs.copyFromLocalFile(new Path(TEST_FILE_SUBJECTS), new Path(
				HDFS_IN_SUBJECTS));
	}

	@Test
	public void testBlankNodeResolution() throws IOException,
			ClassNotFoundException, InterruptedException {
		final Job job = createJob();
		assertTrue("Job should complete successfully", job.waitForCompletion(true));
		final String result = readResults().toString();
		System.err.println("JSON-LD output:\n" + result);
		assertEquals("Expect two lines", 2, result.trim().split("\n").length);
		assertTrue("Expect correct long",
				result.contains("pos#long") && result.contains("2.3377220"));
		assertTrue("Expect correct lat",
				result.contains("pos#lat") && result.contains("48.8681710"));
		assertTrue("Expect correct country name",
				result.contains("ns#country-name") && result.contains("France"));
		assertTrue("Expect correct locality", result.contains("ns#locality")
				&& result.contains("Paris"));
		assertTrue("Expect correct postal code", result.contains("ns#postal-code")
				&& result.contains("75002"));
		assertTrue(
				"Expect correct street-address",
				result.contains("ns#street-address")
						&& result.contains("Rue de Louvois 4"));
		assertTrue("Expect resolved funder type",
				result.contains("Corporate Body or Foundation under Private Law"));
		assertTrue("Expect resolved stock size", result.contains("10,001 - 30,000"));
		assertTrue("Expect resolved type triple for location is not included",
				!result.contains("wgs84_pos#SpatialThing"));
		assertTrue("Expect resolved contributor name",
				result.contains("Zayer, Eric"));
		assertFalse("Unresolved blank node should be filtered",
				result.contains("preferredNameEntityForThePerson"));
	}

	private Job createJob() throws IOException {
		final JobConf conf = createJobConf();
		final String mapFileName = CollectSubjects.mapFileName("testing");
		conf.setStrings("mapred.textoutputformat.separator", " ");
		conf.setStrings(CollectSubjects.PREFIX_KEY, "http://lobid.org/organisation");
		conf.set(NTriplesToJsonLd.INDEX_NAME, "lobid-resources");
		conf.set(NTriplesToJsonLd.INDEX_TYPE, "json-ld-lobid");
		conf.setStrings("map.file.name", mapFileName);
		final URI zippedMapFile =
				CollectSubjects.asZippedMapFile(hdfs, new Path(HDFS_IN_SUBJECTS),
						new Path(HDFS_OUT_ZIP + "/" + mapFileName + ".zip"), conf);
		DistributedCache.addCacheFile(zippedMapFile, conf);
		final Job job = new Job(conf);
		job.setJobName("IntegrationTestLobidNTriplesToJsonLd");
		FileInputFormat.addInputPaths(job, HDFS_IN_TRIPLES_1 + ","
				+ HDFS_IN_TRIPLES_2);
		FileOutputFormat.setOutputPath(job, new Path(HDFS_OUT));
		job.setMapperClass(NTriplesToJsonLdMapper.class);
		job.setReducerClass(NTriplesToJsonLdReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job;
	}

	private StringBuilder readResults() throws IOException {
		final Path[] outputFiles =
				FileUtil.stat2Paths(getFileSystem().listStatus(new Path(HDFS_OUT),
						new Utils.OutputFileUtils.OutputFilesFilter()));
		assertEquals("Expect a single output file", 1, outputFiles.length);
		final StringBuilder builder = new StringBuilder();
		try (final Scanner scanner =
				new Scanner(getFileSystem().open(outputFiles[0]))) {
			while (scanner.hasNextLine())
				builder.append(scanner.nextLine()).append("\n");
		}
		return builder;
	}

	@Override
	@After
	public void tearDown() {
		try {
			hdfs.close();
			super.stopCluster();
		} catch (Exception e) {
			LoggerFactory.getLogger(IntegrationTestLobidNTriplesToJsonLd.class)
					.error(e.getMessage(), e);
		}
	}
}
