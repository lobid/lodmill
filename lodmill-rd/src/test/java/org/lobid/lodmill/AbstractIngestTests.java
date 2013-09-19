/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.SortedSet;
import java.util.TreeSet;

import org.culturegraph.mf.framework.DefaultStreamPipe;
import org.culturegraph.mf.framework.DefaultStreamReceiver;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.morph.MorphErrorHandler;
import org.culturegraph.mf.stream.pipe.ObjectTee;
import org.culturegraph.mf.stream.reader.Reader;
import org.culturegraph.mf.stream.sink.ObjectWriter;
import org.culturegraph.mf.stream.source.FileOpener;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ingest the ZVDD MARC-XML export.
 * 
 * Run as Java application to use metaflow definitions; run as JUnit test to
 * print some stats, transform the fields, and output results as N-Triples.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public abstract class AbstractIngestTests {

	private static final Logger LOG = LoggerFactory
			.getLogger(AbstractIngestTests.class);
	private final String dataFile;
	private final Reader reader;
	protected Metamorph metamorph;
	private final String statsMorphFile;

	public AbstractIngestTests(final String dataFile, final String morphFile,
			final String statsMorphFile, final Reader reader) {
		this.dataFile = dataFile;
		this.statsMorphFile = statsMorphFile;
		metamorph =
				new Metamorph(Thread.currentThread().getContextClassLoader()
						.getResourceAsStream(morphFile));
		this.reader = reader;
	}

	/**
	 * Tests if the generated triples equals the triples in the test file
	 * 
	 * @param testFileName The test file name, residing in the resource folder
	 * @param generatedFileName The to be generated file name .
	 * @param dsp A DefaultStreampipe
	 */
	public void triples(final String testFileName,
			final String generatedFileName,
			final DefaultStreamPipe<ObjectReceiver<String>> dsp) {
		setUpErrorHandler(metamorph);
		final File file = new File(generatedFileName);
		process(dsp, file);
		Scanner actual = null;
		final Scanner expected =
				new Scanner(Thread.currentThread().getContextClassLoader()
						.getResourceAsStream(testFileName));
		try {
			actual = new Scanner(file);
			// Set necessary because the order of triples in the files may differ
			final SortedSet<String> expectedSet = asSet(expected);
			final SortedSet<String> actualSet = asSet(actual);
			assertSetSize(expectedSet, actualSet);
			assertSetElements(expectedSet, actualSet);
			Assert.assertEquals(expectedSet, actualSet);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		} finally {
			if (actual != null) {
				actual.close();
			}
			expected.close();
		}
		file.deleteOnExit();
	}

	private static void assertSetSize(final SortedSet<String> expectedSet,
			final SortedSet<String> actualSet) {
		if (expectedSet.size() != actualSet.size()) {
			final SortedSet<String> missingSet = new TreeSet<String>(expectedSet);
			missingSet.removeAll(actualSet);
			LOG.error("Missing expected result set entries: " + missingSet);
		}
		Assert.assertEquals(expectedSet.size(), actualSet.size());
	}

	private static SortedSet<String> asSet(final Scanner scanner) {
		final SortedSet<String> set = new TreeSet<String>();
		while (scanner.hasNextLine()) {
			final String actual = scanner.nextLine();
			if (!actual.isEmpty()) {
				set.add(actual);
			}
		}
		return set;
	}

	private static void assertSetElements(final SortedSet<String> expectedSet,
			final SortedSet<String> actualSet) {
		final Iterator<String> expectedIterator = expectedSet.iterator();
		final Iterator<String> actualIterator = actualSet.iterator();
		for (int i = 0; i < expectedSet.size(); i++) {
			Assert.assertEquals(expectedIterator.next(), actualIterator.next());
		}
	}

	public void dot(final String fname) {
		setUpErrorHandler(metamorph);
		final File file = new File(fname);
		process(new PipeEncodeDot(), file);
		Assert.assertTrue(file.exists());
		file.deleteOnExit();
	}

	public void stats(final String fileName) throws IOException {
		final File file = new File(fileName);
		metamorph =
				new Metamorph(Thread.currentThread().getContextClassLoader()
						.getResourceAsStream(statsMorphFile));
		setUpErrorHandler(metamorph);
		final Stats stats = new Stats();
		reader.setReceiver(metamorph).setReceiver(stats);
		processFile();
		reader.closeStream();
		final List<Entry<String, Integer>> entries =
				sortedByValuesDescending(stats);
		writeTextileMappingTable(entries, file);
		Assert.assertTrue("We should have some values", entries.size() > 1);
		Assert.assertTrue("Values should have descending frequency", entries.get(0)
				.getValue() >= entries.get(entries.size() - 1).getValue());
		Assert.assertTrue("Mapping table should exist", file.exists());
		file.deleteOnExit();
	}

	protected void process(
			final DefaultStreamPipe<ObjectReceiver<String>> encoder, final File file) {
		final ObjectTee<String> tee = outputTee(file);
		reader.setReceiver(metamorph).setReceiver(encoder).setReceiver(tee);
		processFile();
		reader.closeStream();
		Assert.assertTrue("File should exist", file.exists());
		Assert.assertTrue("File should not be empty", file.length() > 0);
	}

	private void processFile() {
		FileOpener fileOpener = null;
		fileOpener = new FileOpener();
		fileOpener.setReceiver(reader);
		fileOpener.process(dataFile);
	}

	private static ObjectTee<String> outputTee(final File triples) {
		final ObjectTee<String> tee = new ObjectTee<String>();
		tee.addReceiver(new ObjectWriter<String>("stdout"));
		tee.addReceiver(new ObjectWriter<String>(triples.getAbsolutePath()));
		return tee;
	}

	private static class Stats extends DefaultStreamReceiver {

		private final Map<String, Integer> map = new HashMap<String, Integer>();

		@Override
		public void literal(final String name, final String value) {
			map.put(name, (map.containsKey(name) ? map.get(name) : 0) + 1);
		}
	}

	protected static void setUpErrorHandler(final Metamorph metamorph) {
		metamorph.setErrorHandler(new MorphErrorHandler() {
			@Override
			public void error(final Exception exception) {
				LOG.error(exception.getMessage(), exception);
			}
		});
	}

	private static List<Entry<String, Integer>> sortedByValuesDescending(
			final Stats stats) {
		final List<Entry<String, Integer>> entries =
				new ArrayList<Entry<String, Integer>>(stats.map.entrySet());
		Collections.sort(entries, new Comparator<Entry<String, Integer>>() {
			@Override
			public int compare(final Entry<String, Integer> entry1,
					final Entry<String, Integer> entry2) {
				// compare second to first for descending order:
				return entry2.getValue().compareTo(entry1.getValue());
			}
		});
		return entries;
	}

	private static void writeTextileMappingTable(
			final List<Entry<String, Integer>> entries, final File textileMappingFile)
			throws IOException {
		final StringBuilder textileBuilder =
				new StringBuilder(
						"|*field*|*frequency*|*content*|*mapping*|*status*|\n");
		LOG.info("Field\tFreq.");
		LOG.info("----------------");
		for (Entry<String, Integer> e : entries) {
			LOG.info(e.getKey() + "\t" + e.getValue());
			textileBuilder.append(String.format("|%s|%s| | | |\n", e.getKey(),
					e.getValue()));
		}
		final FileWriter textileWriter = new FileWriter(textileMappingFile);
		try {
			textileWriter.write(textileBuilder.toString());
			textileWriter.flush();
		} finally {
			textileWriter.close();
		}
	}
}
