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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.culturegraph.mf.framework.DefaultStreamReceiver;
import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.morph.MorphErrorHandler;
import org.culturegraph.mf.stream.pipe.ObjectTee;
import org.culturegraph.mf.stream.reader.Reader;
import org.culturegraph.mf.stream.sink.ObjectWriter;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ingest the ZDB PICA-XML export.
 * 
 * Run as Java application to use metaflow definitions; run as JUnit test to
 * print some stats, transform the fields, and output results as N-Triples.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public final class LobidOrganisationPicaIngest {

	private static final String TEXTILE_MAPPING_TABLE = "mapping.textile";
	private static final Logger LOG = LoggerFactory
			.getLogger(LobidOrganisationPicaIngest.class);
	private static final String LOBID_ORGA_PICA =
			"transformations/lobid-organisation/Bibdat1303pp_sample1.xml";
	private final Reader reader = new PicaXmlReader();
	private Metamorph metamorph = new Metamorph(Thread.currentThread()
			.getContextClassLoader()
			.getResourceAsStream("morph_zdb-isil-file-pica2ld.xml"));

	// @Test
	public void stats() throws IOException {
		// "zvdd-morph-stats.xml" is also fine for zdb-adress-file stats
		metamorph =
				new Metamorph(Thread.currentThread().getContextClassLoader()
						.getResourceAsStream("zvdd_morph-stats.xml"));
		setUpErrorHandler(metamorph);
		final ZvddStats stats = new ZvddStats();
		reader.setReceiver(metamorph).setReceiver(stats);
		try (FileReader fileReader = new FileReader(LOBID_ORGA_PICA)) {
			reader.process(fileReader);
		}
		final List<Entry<String, Integer>> entries =
				sortedByValuesDescending(stats);
		final File mapping = writeTextileMappingTable(entries);

		Assert.assertTrue("We should have some values", entries.size() > 1);
		Assert.assertTrue("Values should have descending frequency", entries.get(0)
				.getValue() > entries.get(entries.size() - 1).getValue());
		Assert.assertTrue("Mapping table should exist", mapping.exists());
		// / mapping.deleteOnExit();
	}

	@Test
	public void triples() {
		setUpErrorHandler(metamorph);
		process(new PipeEncodeTriples(), new File("zdb-isil-file.nt"));
	}

	// @Test
	public void dot() {
		setUpErrorHandler(metamorph);
		process(new PipeEncodeDot(), new File("zdb-isil-file.dot"));
	}

	private void process(final AbstractGraphPipeEncoder encoder, final File file) {
		final ObjectTee<String> tee = outputTee(file);
		reader.setReceiver(metamorph).setReceiver(encoder).setReceiver(tee);
		try (FileReader fileReader = new FileReader(LOBID_ORGA_PICA)) {
			reader.process(fileReader);
		} catch (IOException e) {
			e.printStackTrace();
		}
		reader.closeStream();
		/*
		 * Assert.assertTrue("File should exist", file.exists());
		 * Assert.assertTrue("File should not be empty", file.length() > 0);
		 * file.deleteOnExit();
		 */}

	private static ObjectTee<String> outputTee(final File triples) {
		final ObjectTee<String> tee = new ObjectTee<>();
		tee.addReceiver(new ObjectWriter<String>("stdout"));
		tee.addReceiver(new ObjectWriter<String>(triples.getAbsolutePath()));
		return tee;
	}

	private static class ZvddStats extends DefaultStreamReceiver {

		private Map<String, Integer> map = new HashMap<>();

		@Override
		public void literal(final String name, final String value) {
			map.put(name, (map.containsKey(name) ? map.get(name) : 0) + 1);
		}
	}

	private static void setUpErrorHandler(Metamorph metamorph) {
		metamorph.setErrorHandler(new MorphErrorHandler() {
			@Override
			public void error(final Exception exception) {
				LOG.error(exception.getMessage(), exception);
			}
		});
	}

	private static List<Entry<String, Integer>> sortedByValuesDescending(
			final ZvddStats stats) {
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

	private static File writeTextileMappingTable(
			final List<Entry<String, Integer>> entries) throws IOException {
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
		final File mapping = new File(TEXTILE_MAPPING_TABLE);
		try (FileWriter textileWriter = new FileWriter(mapping)) {
			textileWriter.write(textileBuilder.toString());
			textileWriter.flush();
		}
		return mapping;
	}
}
