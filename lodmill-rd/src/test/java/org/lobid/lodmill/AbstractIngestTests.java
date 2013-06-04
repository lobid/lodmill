/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.File;
import java.io.FileReader;
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
	private String statsMorphFile;

	public AbstractIngestTests(String dataFile, String morphFile,
			String statsMorphFile, Reader reader) {
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
	 * @param testFileName The test file name, resideing in the resource folder
	 * @param generatedFileName The to be generated file name .
	 * @param dsp A DefaultStreampipe
	 */
	public void triples(String testFileName, String generatedFileName,
			DefaultStreamPipe<ObjectReceiver<String>> dsp) {
		setUpErrorHandler(metamorph);
		final File file = new File(generatedFileName);
		process(dsp, file);
		try (Scanner actual = new Scanner(file);
				Scanner expected =
						new Scanner(Thread.currentThread().getContextClassLoader()
								.getResourceAsStream(testFileName))) {
			// Set necessary because the order of triples in the files may differ
			SortedSet<String> expectedSet = asSet(expected);
			SortedSet<String> actualSet = asSet(actual);
			assertSetSize(expectedSet, actualSet);
			assertSetElements(expectedSet, actualSet);
			Assert.assertEquals(expectedSet, actualSet);
		} catch (IOException e) {
			e.printStackTrace();
		}
		file.deleteOnExit();
	}

	private static void assertSetSize(SortedSet<String> expectedSet,
			SortedSet<String> actualSet) {
		if (expectedSet.size() != actualSet.size()) {
			SortedSet<String> missingSet = new TreeSet<>(expectedSet);
			missingSet.removeAll(actualSet);
			LOG.error("Missing expected result set entries: " + missingSet);
		}
		Assert.assertEquals(expectedSet.size(), actualSet.size());
	}

	private static SortedSet<String> asSet(Scanner scanner) {
		SortedSet<String> set = new TreeSet<>();
		while (scanner.hasNextLine()) {
			String actual = scanner.nextLine();
			if (!actual.isEmpty()) {
				set.add(actual);
			}
		}
		return set;
	}

	private static void assertSetElements(SortedSet<String> expectedSet,
			SortedSet<String> actualSet) {
		Iterator<String> expectedIterator = expectedSet.iterator();
		Iterator<String> actualIterator = actualSet.iterator();
		for (int i = 0; i < expectedSet.size(); i++) {
			Assert.assertEquals(expectedIterator.next(), actualIterator.next());
		}
	}

	public void dot(String fname) {
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
		try (FileReader fileReader = new FileReader(dataFile)) {
			reader.process(fileReader);
		}
		final List<Entry<String, Integer>> entries =
				sortedByValuesDescending(stats);
		writeTextileMappingTable(entries, file);
		Assert.assertTrue("We should have some values", entries.size() > 1);
		Assert.assertTrue("Values should have descending frequency", entries.get(0)
				.getValue() > entries.get(entries.size() - 1).getValue());
		Assert.assertTrue("Mapping table should exist", file.exists());
		file.deleteOnExit();
	}

	protected void process(
			final DefaultStreamPipe<ObjectReceiver<String>> encoder, final File file) {
		final ObjectTee<String> tee = outputTee(file);
		reader.setReceiver(metamorph).setReceiver(encoder).setReceiver(tee);
		try (FileReader fileReader = new FileReader(dataFile)) {
			reader.process(fileReader);
		} catch (IOException e) {
			e.printStackTrace();
		}
		reader.closeStream();
		Assert.assertTrue("File should exist", file.exists());
		Assert.assertTrue("File should not be empty", file.length() > 0);
	}

	private static ObjectTee<String> outputTee(final File triples) {
		final ObjectTee<String> tee = new ObjectTee<>();
		tee.addReceiver(new ObjectWriter<String>("stdout"));
		tee.addReceiver(new ObjectWriter<String>(triples.getAbsolutePath()));
		return tee;
	}

	private static class Stats extends DefaultStreamReceiver {

		private Map<String, Integer> map = new HashMap<>();

		@Override
		public void literal(final String name, final String value) {
			map.put(name, (map.containsKey(name) ? map.get(name) : 0) + 1);
		}
	}

	protected static void setUpErrorHandler(Metamorph metamorph) {
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
				new ArrayList<>(stats.map.entrySet());
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
		StringBuilder textileBuilder =
				new StringBuilder(
						"|*field*|*frequency*|*content*|*mapping*|*status*|\n");
		LOG.info("Field\tFreq.");
		LOG.info("----------------");
		for (Entry<String, Integer> e : entries) {
			LOG.info(e.getKey() + "\t" + e.getValue());
			textileBuilder.append(String.format("|%s|%s| | | |\n", e.getKey(),
					e.getValue()));
		}
		try (FileWriter textileWriter = new FileWriter(textileMappingFile)) {
			textileWriter.write(textileBuilder.toString());
			textileWriter.flush();
		}
	}
}
