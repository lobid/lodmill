package org.lobid.lodmill;

import java.io.File;
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
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple field statistics .
 * 
 * @author Pascal Christoph
 * @author Fabian Steeg (fsteeg)
 * 
 */
@Description("Simple sorted field statistics. The parametre 'filename' defines the place to store the stats on disk.")
@In(StreamReceiver.class)
@Out(Void.class)
public final class Stats extends DefaultStreamReceiver {
	private static final Logger LOG = LoggerFactory
			.getLogger(DefaultStreamReceiver.class);
	final Map<String, Integer> map = new HashMap<String, Integer>();
	private String filename;

	/**
	 * Default constructor
	 */
	public Stats() {
		this.filename = "tmp.stats.csv";
	}

	/**
	 * Sets the filename for writing the statistics.
	 * 
	 * @param filename the filename
	 */
	public void setFilename(final String filename) {
		this.filename = filename;
	}

	@Override
	public void literal(final String name, final String value) {
		map.put(name, (map.containsKey(name) ? map.get(name) : 0) + 1);
	}

	@Override
	public void closeStream() {
		try {
			writeTextileMappingTable(sortedByValuesDescending(), new File(
					this.filename));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static void writeTextileMappingTable(
			final List<Entry<String, Integer>> entries, final File textileMappingFile)
			throws IOException {
		final StringBuilder textileBuilder =
				new StringBuilder("|*field*|*frequency*|\n");
		LOG.info("Field\tFreq.");
		LOG.info("----------------");
		for (Entry<String, Integer> e : entries) {
			LOG.info(e.getKey() + "\t" + e.getValue());
			textileBuilder
					.append(String.format("|%s|%s|\n", e.getKey(), e.getValue()));
		}
		final FileWriter textileWriter = new FileWriter(textileMappingFile);
		try {
			textileWriter.write(textileBuilder.toString());
			textileWriter.flush();
		} finally {
			textileWriter.close();
		}
	}

	List<Entry<String, Integer>> sortedByValuesDescending() {
		final List<Entry<String, Integer>> entries =
				new ArrayList<Entry<String, Integer>>(map.entrySet());
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
