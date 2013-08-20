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
	private static final String TEST_FILE_TRIPLES =
			"src/test/resources/lobid-org-with-blank-nodes.nt";
	private static final String TEST_FILE_SUBJECTS =
			"src/test/resources/lobid-org-required-subjects.out";
	private static final String HDFS_IN_TRIPLES = "blank-nodes-test/sample.nt";
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
		hdfs.copyFromLocalFile(new Path(TEST_FILE_TRIPLES), new Path(
				HDFS_IN_TRIPLES));
		hdfs.copyFromLocalFile(new Path(TEST_FILE_SUBJECTS), new Path(
				HDFS_IN_SUBJECTS));
	}

	@Test
	public void testBlankNodeResolution() throws IOException,
			ClassNotFoundException, InterruptedException {
		final Job job = createJob();
		assertTrue("Job should complete successfully", job.waitForCompletion(true));
		final String string = readResults().toString();
		System.err.println("JSON-LD output:\n" + string);
		assertTrue("Expect long", string.contains("pos#long"));
		assertTrue("Expect lat", string.contains("pos#lat"));
		assertTrue("Expect county", string.contains("ns#country-name"));
		assertTrue("Expect locality", string.contains("ns#locality"));
		assertTrue("Expect postal", string.contains("ns#postal-code"));
		assertTrue("Expect adr", string.contains("ns#street-address"));
	}

	private Job createJob() throws IOException {
		final JobConf conf = createJobConf();
		conf.setStrings("mapred.textoutputformat.separator", " ");
		final URI zippedMapFile =
				CollectSubjects.asZippedMapFile(hdfs, new Path(HDFS_IN_SUBJECTS),
						new Path(HDFS_OUT_ZIP + "/" + CollectSubjects.MAP_FILE_ZIP));
		DistributedCache.addCacheFile(zippedMapFile, conf);
		final Job job = new Job(conf);
		job.setJobName("IntegrationTestLobidNTriplesToJsonLd");
		FileInputFormat.addInputPaths(job, HDFS_IN_TRIPLES);
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
