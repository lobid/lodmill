/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.culturegraph.metamorph.reader.MarcXmlReader;
import org.culturegraph.metamorph.reader.Reader;
import org.culturegraph.metastream.framework.DefaultStreamReceiver;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ingest the ZVDD MARC-XML export and print some stats.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class ZvddMarcIngest {

	private static final Logger LOG = LoggerFactory
			.getLogger(ZvddMarcIngest.class);
	private static final String ZVDD_MARC = "../../zvdd.xml";
	private final Reader reader = new MarcXmlReader();

	private static class ZvddStats extends DefaultStreamReceiver {

		private String lastEntity;
		private Map<String, Integer> map = new HashMap<>();

		@Override
		public void startEntity(final String name) {
			lastEntity = name;
		}

		@Override
		public void endEntity() {
			lastEntity = null;
		}

		@Override
		public void literal(final String name, final String value) {
			final String field =
					(lastEntity == null ? "" : lastEntity.trim()) + "-" + name;
			map.put(field, (map.containsKey(field) ? map.get(field) : 0) + 1);
		}
	}

	@Test
	public void ingest() throws IOException {
		final ZvddStats stats = new ZvddStats();
		reader.setReceiver(stats);
		reader.process(new FileReader(ZVDD_MARC));
		final List<Entry<String, Integer>> entries =
				sortedByValuesDescending(stats);
		LOG.info("Field\tFreq.");
		LOG.info("----------------");
		for (Entry<String, Integer> e : entries) {
			LOG.info(e.getKey() + "\t" + e.getValue());
		}
		Assert.assertTrue("We should have some values", entries.size() > 1);
		Assert.assertTrue("Values should have descending frequency", entries
				.get(0).getValue() > entries.get(entries.size() - 1).getValue());
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
}
