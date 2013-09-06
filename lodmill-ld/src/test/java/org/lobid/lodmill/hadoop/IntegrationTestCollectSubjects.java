/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.fs.FileStatus;
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
import org.lobid.lodmill.hadoop.CollectSubjects.CollectSubjectsMapper;
import org.lobid.lodmill.hadoop.CollectSubjects.CollectSubjectsReducer;
import org.slf4j.LoggerFactory;

/**
 * Test {@link #CollectSubjects} job with blank nodes.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public class IntegrationTestCollectSubjects extends ClusterMapReduceTestCase {
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
		final String string = readResults().toString();
		System.err.println("Collection output:\n" + string);
		assertEquals(
				"_:node16vicghfdx1 http://lobid.org/organisation/ACRPP,http://lobid.org/organisation/AAAAA\n"
						+ "_:node16vicghfdx2 http://lobid.org/organisation/ACRPP\n"
						+ "http://purl.org/lobid/fundertype#n08 http://lobid.org/organisation/ACRPP\n"
						+ "http://purl.org/lobid/stocksize#n06 http://lobid.org/organisation/ACRPP\n",
				string);
		writeZippedMapFile();
	}

	private void writeZippedMapFile() throws IOException {
		long time = System.currentTimeMillis();
		final Path[] outputFiles =
				FileUtil.stat2Paths(getFileSystem().listStatus(new Path(HDFS_OUT),
						new Utils.OutputFileUtils.OutputFilesFilter()));
		final Path zipOutputLocation =
				new Path(HDFS_OUT + "/" + CollectSubjects.MAP_FILE_ZIP);
		CollectSubjects.asZippedMapFile(hdfs, outputFiles[0], zipOutputLocation);
		final FileStatus fileStatus = hdfs.getFileStatus(zipOutputLocation);
		assertTrue(fileStatus.getModificationTime() >= time);
	}

	private Job createJob() throws IOException {
		final JobConf conf = createJobConf();
		conf.setStrings("mapred.textoutputformat.separator", " ");
		conf.setStrings(CollectSubjects.PREFIX_KEY, "http://lobid.org/organisation");
		final Job job = new Job(conf);
		job.setJobName("CollectSubjects");
		FileInputFormat.addInputPaths(job, HDFS_IN);
		FileOutputFormat.setOutputPath(job, new Path(HDFS_OUT));
		job.setMapperClass(CollectSubjectsMapper.class);
		job.setReducerClass(CollectSubjectsReducer.class);
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
			LoggerFactory.getLogger(IntegrationTestCollectSubjects.class).error(
					e.getMessage(), e);
		}
	}
}
