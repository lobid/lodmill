/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStreamReader;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

/**
 * Convert lobid.org and GND N-Triples to JSON-LD with two Hadoop jobs and bulk
 * index in ElasticSearch using command-line tools (split, curl) and
 * ProcessRunner.
 * <p/>
 * This is mainly for local testing of the Hadoop job and their output format
 * (it uses local input files and assumes local output files, on which the CLI
 * tools operate). A production workflow would use data in HDFS.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class LobidToElasticSearchLocal {

	private static final String LOCALHOST = "localhost:9200";

	public static void main(final String[] args) {
		final LobidToElasticSearchLocal importer =
				new LobidToElasticSearchLocal();

		if (args.length < 2) {
			System.err.println("Params: <input> <output> [<server>]");
			System.exit(-1);
		}

		final String input = args[0];
		final String output = args[1];
		final String inter = new File(output).getParent() + "/inter";
		final String server = args.length == 3 ? args[2] : LOCALHOST;

		importer.runHadoopJobs(input, inter, output);
		importer.indexInElasticSearch(output, server);
	}

	private void indexInElasticSearch(final String output, final String server) {
		for (final File file : new File(output).listFiles(new PartFilter())) {
			final ProcessBuilder splitProcessBuilder =
					new ProcessBuilder("split", "-l 10",
							file.getAbsolutePath(), "segment-" + file.getName()
									+ "-");
			run(splitProcessBuilder, file.getParentFile());
			for (File seg : new File(output).listFiles(new SegmentFilter(file))) {
				final ProcessBuilder curlProcessBuilder =
						new ProcessBuilder("curl", "-s", "-XPOST", server
								+ "/_bulk", "--data-binary", "@"
								+ seg.getName());
				run(curlProcessBuilder, seg.getParentFile());
			}
		}
	}

	static class PartFilter implements FileFilter {
		@Override
		public boolean accept(final File pathname) {
			return pathname.getName().startsWith("part-");
		}
	}

	static class SegmentFilter implements FileFilter {
		SegmentFilter(final File file) {
			this.file = file;
		}

		private final File file;

		@Override
		public boolean accept(final File pathname) {
			return pathname.getName().startsWith("segment-" + file.getName());
		}
	}

	private void run(final ProcessBuilder processBuilder, final File work) {
		processBuilder.directory(work);
		try {
			final Process splitProcess = processBuilder.start();
			splitProcess.waitFor();
			printOutput(splitProcess);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace(); // NOPMD
		}
	}

	private void runHadoopJobs(final String inputPath, final String interPath,
			final String outputPath) {
		try {
			ResolveObjectUrisInLobidNTriples.main(new String[] { inputPath,
					interPath });
			NTriplesToJsonLd.main(new String[] { interPath, outputPath });
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void printOutput(final Process process) throws IOException {
		final String errors =
				CharStreams.toString(new InputStreamReader(process
						.getErrorStream(), Charsets.UTF_8));
		if (!errors.trim().isEmpty()) {
			System.err.println("Error: " + errors);
		}
		final String infos =
				CharStreams.toString(new InputStreamReader(process
						.getInputStream(), Charsets.UTF_8));
		if (!infos.trim().isEmpty()) {
			System.out.println("Info: " + infos);
		}
	}
}
