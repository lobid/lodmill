package org.lobid.lodmill;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
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
 * Field statistics. Instead of occurences a comma separated list of values may
 * be output.
 * 
 * @author Pascal Christoph
 * @author Fabian Steeg (fsteeg)
 * 
 */
@Description("Sorted field statistics. May have appended a list of all values which "
		+ "are part of a field. The parameter 'filename' defines the place to store the"
		+ " stats on disk.")
@In(StreamReceiver.class)
@Out(Void.class)
public final class Stats extends DefaultStreamReceiver {
	private static final Logger LOG = LoggerFactory
			.getLogger(DefaultStreamReceiver.class);
	final HashMap<String, Integer> map = new HashMap<>();
	final HashMap<String, StringBuilder> map_values = new HashMap<>();
	private String filename;
	private static FileWriter textileWriter;

	/**
	 * Default constructor
	 */
	public Stats() {
		this.filename =
				"tmp.stats." + (Calendar.getInstance().getTimeInMillis() / 1000)
						+ ".csv";
	}

	/**
	 * Sets the filename for writing the statistics.
	 * 
	 * @param filename the filename
	 */
	public void setFilename(final String filename) {
		this.filename = filename;
	}

	/**
	 * Counts occurences of fields. If field name starts with "log:", not the
	 * occurence are counted but the values are concatenated.
	 */
	@Override
	public void literal(final String name, final String value) {
		if (name.startsWith("log:")) {
			map_values.put(name, (map_values.containsKey(name) ? map_values.get(name)
					.append("," + value) : new StringBuilder(value)));
		} else
			map.put(name, (map.containsKey(name) ? map.get(name) : 0) + 1);
	}

	@Override
	public void closeStream() {
		try {
			writeTextileMappingTable(sortedByValuesDescending(), map_values,
					new File(this.filename));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static void writeTextileMappingTable(
			final List<Entry<String, Integer>> entries,
			Map<String, StringBuilder> map_values, final File textileMappingFile)
			throws IOException {
		final StringBuilder textileBuilder =
				new StringBuilder(
						"|*field*|*frequency or values separated with commata*|\n");
		LOG.info("Field\tFrequency or comma separated values");
		LOG.info("----------------");
		for (Entry<String, Integer> e : entries) {
			LOG.info(e.getKey() + "\t" + e.getValue());
			textileBuilder
					.append(String.format("|%s|%s|\n", e.getKey(), e.getValue()));
		}
		for (Entry<String, StringBuilder> e : map_values.entrySet()) {
			LOG.info(e.getKey() + "\t" + e.getValue());
			textileBuilder
					.append(String.format("|%s|%s|\n", e.getKey(), e.getValue()));
		}
		textileWriter = new FileWriter(textileMappingFile);
		try {
			textileWriter.write(textileBuilder.toString());
			textileWriter.flush();
		} finally {
			textileWriter.close();
		}
	}

	List<Entry<String, Integer>> sortedByValuesDescending() {
		final List<Entry<String, Integer>> entries =
				new ArrayList<>(map.entrySet());
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
