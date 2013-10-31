/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.SortedSet;
import java.util.TreeSet;

import org.culturegraph.mf.framework.DefaultStreamPipe;
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
		final File actualFile = new File(generatedFileName);
		process(dsp, actualFile);
		File testFile;
		try {
			testFile =
					new File(Thread.currentThread().getContextClassLoader()
							.getResource(testFileName).toURI());
			compareFiles(actualFile, testFile);
		} catch (URISyntaxException e) {
			LOG.error(e.getMessage(), e);
		}

	}

	/**
	 * Tests if two files are of equal content.
	 * 
	 * @param actualFile the actually generated file
	 * @param testFile the file which defines how the actualFile should look like
	 */
	public static void compareFiles(final File actualFile, final File testFile) {
		Scanner actual = null;
		Scanner expected = null;
		try {
			expected = new Scanner(testFile);
			actual = new Scanner(actualFile);
			// Set necessary because the order of triples in the files may differ
			final SortedSet<String> expectedSet = asSet(expected);
			final SortedSet<String> actualSet = asSet(actual);
			assertSetSize(expectedSet, actualSet);
			assertSetElements(expectedSet, actualSet);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		} finally {
			if (actual != null) {
				actual.close();
			}
			if (expected != null) {
				expected.close();
			}
		}
		actualFile.deleteOnExit();
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
			String expected = expectedIterator.next();
			String actual = actualIterator.next();
			if (!expected.equals(actual)) {
				LOG.error("Expected: " + expected + "\n but was:" + actual);
			}
		}
		Assert.assertEquals(expectedSet, actualSet);
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
				stats.sortedByValuesDescending();
		Stats.writeTextileMappingTable(entries, file);
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

	protected static void setUpErrorHandler(final Metamorph metamorph) {
		metamorph.setErrorHandler(new MorphErrorHandler() {
			@Override
			public void error(final Exception exception) {
				LOG.error(exception.getMessage(), exception);
			}
		});
	}
}
