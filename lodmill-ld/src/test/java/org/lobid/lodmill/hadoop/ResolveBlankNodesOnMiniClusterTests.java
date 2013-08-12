/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import java.io.IOException;
import java.util.Scanner;

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
import org.lobid.lodmill.hadoop.ResolveObjectUrisInLobidNTriples.ResolveTriplesMapper;
import org.lobid.lodmill.hadoop.ResolveObjectUrisInLobidNTriples.ResolveTriplesReducer;
import org.slf4j.LoggerFactory;

/**
 * Test {@link #ResolveObjectUrisInLobidNTriples} job with blank nodes.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public class ResolveBlankNodesOnMiniClusterTests extends
		ClusterMapReduceTestCase {
	private static final String TEST_FILE =
			"src/test/resources/lobid-org-with-blank-nodes.nt";
	private static final String HDFS_IN = "blank-nodes-test/sample.nt";
	private static final String HDFS_OUT = "out/sample";
	private FileSystem hdfs = null;

	@Before
	@Override
	public void setUp() throws Exception {
		System.setProperty("hadoop.log.dir", "/tmp/logs");
		super.setUp();
		hdfs = getFileSystem();
		hdfs.copyFromLocalFile(new Path(TEST_FILE), new Path(HDFS_IN));
	}

	@Test
	public void testBlankNodeResolution() throws IOException,
			ClassNotFoundException, InterruptedException {
		final Job job = createJob();
		assertTrue("Job should complete successfully", job.waitForCompletion(true));
		final StringBuilder builder = readResults();
		System.err.println("Resolved triples:\n" + builder.toString());
		assertTrue("Expect long", builder.toString().contains("pos#long"));
		assertTrue("Expect lat", builder.toString().contains("pos#lat"));
		assertTrue("Expect county", builder.toString().contains("ns#country-name"));
		assertTrue("Expect locality", builder.toString().contains("ns#locality"));
		assertTrue("Expect postal", builder.toString().contains("ns#postal-code"));
		assertTrue("Expect adr", builder.toString().contains("ns#street-address"));
	}

	private Job createJob() throws IOException {
		final JobConf conf = createJobConf();
		conf.setStrings("mapred.textoutputformat.separator", " ");
		final Job job = new Job(conf);
		job.setJobName("ResolveObjectUrisInLobidNTriples");
		FileInputFormat.addInputPaths(job, HDFS_IN);
		FileOutputFormat.setOutputPath(job, new Path(HDFS_OUT));
		job.setMapperClass(ResolveTriplesMapper.class);
		job.setReducerClass(ResolveTriplesReducer.class);
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
		try (final Scanner scanner = new Scanner(hdfs.open(outputFiles[0]))) {
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
			LoggerFactory.getLogger(ResolveBlankNodesOnMiniClusterTests.class).error(
					e.getMessage(), e);
		}
	}
}
