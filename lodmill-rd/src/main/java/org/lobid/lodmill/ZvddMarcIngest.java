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

import org.antlr.runtime.RecognitionException;
import org.culturegraph.metaflow.Metaflow;
import org.culturegraph.metamorph.core.Metamorph;
import org.culturegraph.metamorph.core.MetamorphErrorHandler;
import org.culturegraph.metamorph.reader.MarcXmlReader;
import org.culturegraph.metamorph.reader.Reader;
import org.culturegraph.metastream.framework.DefaultStreamReceiver;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ingest the ZVDD MARC-XML export and print some stats.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class ZvddMarcIngest {

	private static final String TEXTILE_MAPPING_TABLE = "mapping.textile";
	private static final Logger LOG = LoggerFactory
			.getLogger(ZvddMarcIngest.class);
	private static final String ZVDD_MARC = "../../zvdd.xml";
	private final Reader reader = new MarcXmlReader();
	private final Metamorph metamorph = new Metamorph(Thread.currentThread()
			.getContextClassLoader().getResourceAsStream("morph-stats.xml"));

	private static class ZvddStats extends DefaultStreamReceiver {

		private Map<String, Integer> map = new HashMap<>();

		@Override
		public void literal(final String name, final String value) {
			map.put(name, (map.containsKey(name) ? map.get(name) : 0) + 1);
		}
	}

	public static void main(String[] args) throws IOException,
			RecognitionException {
		Metaflow.main(new String[] { "-f", "src/main/resources/zvdd.flow" });
	}

	@Test
	public void ingest() throws IOException {
		metamorph.setErrorHandler(new MetamorphErrorHandler() {
			@Override
			public void error(final Exception exception) {
				LOG.error(exception.getMessage(), exception);
			}
		});
		final ZvddStats stats = new ZvddStats();
		reader.setReceiver(metamorph).setReceiver(stats);
		reader.process(new FileReader(ZVDD_MARC));
		final List<Entry<String, Integer>> entries =
				sortedByValuesDescending(stats);
		final File mapping = writeTextileMappingTable(entries);
		Assert.assertTrue("We should have some values", entries.size() > 1);
		Assert.assertTrue("Values should have descending frequency", entries
				.get(0).getValue() > entries.get(entries.size() - 1).getValue());
		Assert.assertTrue("Mapping table should exist", mapping.exists());
		mapping.deleteOnExit();
	}

	private List<Entry<String, Integer>> sortedByValuesDescending(
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

	private File writeTextileMappingTable(
			final List<Entry<String, Integer>> entries) throws IOException {
		StringBuilder textileBuilder =
				new StringBuilder("|*field*|*frequency*|*content*|*mapping*|\n");
		LOG.info("Field\tFreq.");
		LOG.info("----------------");
		for (Entry<String, Integer> e : entries) {
			LOG.info(e.getKey() + "\t" + e.getValue());
			textileBuilder.append(String.format("|%s|%s| | |\n", e.getKey(),
					e.getValue()));
		}
		final File mapping = new File(TEXTILE_MAPPING_TABLE);
		try (FileWriter textileWriter = new FileWriter(mapping)) {
			textileWriter.write(textileBuilder.toString());
		}
		return mapping;
	}
}
