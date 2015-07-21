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
 * @author Pascal Christoph (dr0i)
 * @author Fabian Steeg (fsteeg)
 * 
 */
@Description("Sorted field statistics. May have appended a list of all values which "
		+ "are part of a field. The parameter 'filename' defines the place to store the"
		+ " stats on disk.")
@In(StreamReceiver.class)
@Out(Void.class)
public final class Stats extends DefaultStreamReceiver {
	private static final Logger LOG =
			LoggerFactory.getLogger(DefaultStreamReceiver.class);
	final HashMap<String, Integer> occurenceMap = new HashMap<>();
	final HashMap<String, StringBuilder> valueMap = new HashMap<>();
	private String filename;
	private static FileWriter textileWriter;

	/**
	 * Default constructor
	 */
	public Stats() {
		this.filename =
				"stats." + (Calendar.getInstance().getTimeInMillis() / 1000) + ".csv";
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
	 * Since the default name file this class produces is rather unique it
	 * should be removable, especially when running as a test
	 * 
	 */
	public void removeTestFile() {
		new File(this.filename).deleteOnExit();
	}

	/**
	 * Counts occurences of fields. If field name starts with "log:", not the
	 * occurence are counted but the values are concatenated.
	 */
	@Override
	public void literal(final String name, final String value) {
		if (name.startsWith("log:")) {
			valueMap
			.put(name,
					(valueMap.containsKey(name)
							? valueMap.get(name).append("," + value)
									: new StringBuilder(value)));
		} else
			occurenceMap.put(name,
					(occurenceMap.containsKey(name) ? occurenceMap.get(name) : 0) + 1);
	}

	@Override
	public void closeStream() {
		try {
			writeTextileMappingTable(sortedByValuesDescending(),
					new ArrayList<>(valueMap.entrySet()), new File(this.filename));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static void writeTextileMappingTable(
			final List<Entry<String, Integer>> occurenceEntries,
			final List<Entry<String, StringBuilder>> valueEntries,
			final File textileMappingFile) throws IOException {
		final StringBuilder textileBuilder = new StringBuilder(
				"|*field*|*frequency or values separated with commata*|\n");
		LOG.info("Field\tFrequency or comma separated values");
		LOG.info("----------------");
		createCsv(occurenceEntries, textileBuilder);
		createCsv(valueEntries, textileBuilder);
		textileWriter = new FileWriter(textileMappingFile);
		try {
			textileWriter.write(textileBuilder.toString());
			textileWriter.flush();
		} finally {
			textileWriter.close();
		}
	}

	private static <T, I> void createCsv(final List<Entry<T, I>> entries,
			final StringBuilder textileBuilder) {
		entries.forEach(e -> {
			LOG.info(e.getKey() + "\t" + e.getValue());
			textileBuilder
			.append(String.format("|%s|%s|\n", e.getKey(), e.getValue()));
		});
	}

	List<Entry<String, Integer>> sortedByValuesDescending() {
		final List<Entry<String, Integer>> entries =
				new ArrayList<>(occurenceMap.entrySet());
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
