/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop; // NOPMD

import static org.lobid.lodmill.hadoop.UnitTestCollectSubjects.GND_CREATOR_ID;
import static org.lobid.lodmill.hadoop.UnitTestCollectSubjects.gnd;
import static org.lobid.lodmill.hadoop.UnitTestLobidNTriplesToJsonLd.indexMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.TestDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.json.simple.JSONValue;
import org.junit.Before;
import org.junit.Test;
import org.lobid.lodmill.hadoop.NTriplesToJsonLd.NTriplesToJsonLdMapper;
import org.lobid.lodmill.hadoop.NTriplesToJsonLd.NTriplesToJsonLdReducer;

/**
 * Test the {@link NTriplesToJsonLd} class.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public final class UnitTestGndNTriplesToJsonLd {

	private static final String TRIPLE_ID = "http://d-nb.info/gnd/"
			+ GND_CREATOR_ID;
	private static final String TRIPLE_URI = "<" + TRIPLE_ID + ">";
	static final String GND_TRIPLE_1 = gnd("preferredNameForThePerson",
			"Adamucci, Antonio");
	static final String GND_TRIPLE_2 = gnd("dateOfBirth", "1828");
	static final String GND_TRIPLE_3 = gnd("dateOfDeath", "1885");
	private static final String INDEX = "gnd-index";
	private static final String TYPE = "json-ld-gnd";
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		final Configuration configuration = new Configuration();
		configuration.set(NTriplesToJsonLd.INDEX_NAME, INDEX);
		configuration.set(NTriplesToJsonLd.INDEX_TYPE, TYPE);
		final NTriplesToJsonLdMapper mapper = new NTriplesToJsonLdMapper();
		final NTriplesToJsonLdReducer reducer = new NTriplesToJsonLdReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		setConfiguration(mapDriver);
		setConfiguration(reduceDriver);
	}

	private static void setConfiguration(final TestDriver<?, ?, ?, ?, ?> driver) {
		driver.getConfiguration().set(NTriplesToJsonLd.INDEX_NAME, INDEX);
		driver.getConfiguration().set(NTriplesToJsonLd.INDEX_TYPE, TYPE);
	}

	@Test
	public void testMapper() throws IOException { // NOPMD (MRUnit)
		mapDriver.addInput(new LongWritable(), new Text(GND_TRIPLE_1));
		mapDriver.addInput(new LongWritable(), new Text(GND_TRIPLE_2));
		mapDriver.addInput(new LongWritable(), new Text(GND_TRIPLE_3));
		mapDriver.withOutput(new Text(TRIPLE_URI), new Text(GND_TRIPLE_1))
				.withOutput(new Text(TRIPLE_URI), new Text(GND_TRIPLE_2))
				.withOutput(new Text(TRIPLE_URI), new Text(GND_TRIPLE_3));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException { // NOPMD (MRUnit)
		reduceDriver.withInput(new Text(TRIPLE_URI), Arrays.asList(new Text(
				GND_TRIPLE_1), new Text(GND_TRIPLE_2), new Text(GND_TRIPLE_3)));
		reduceDriver.withOutput(
				new Text(JSONValue.toJSONString(indexMap(INDEX, TYPE, TRIPLE_ID))),
				new Text(JSONValue.toJSONString(jsonMap())));
		reduceDriver.runTest();
	}

	@SuppressWarnings("serial")
	/* using static init for better readability of nested result structure */
	static Map<String, ?> jsonMap() {
		final String idKey = "@id";// @formatter:off
		final Map<String, Object> json = new HashMap<String, Object>() {{//NOPMD
			put("@graph", Arrays.asList(new HashMap<String, Object>() {{//NOPMD
				put(idKey, TRIPLE_ID);
				put("http://d-nb.info/standards/elementset/gnd#preferredNameForThePerson",
						"Adamucci, Antonio");
				put("http://d-nb.info/standards/elementset/gnd#dateOfDeath", "1885");
				put("http://d-nb.info/standards/elementset/gnd#dateOfBirth", "1828");
			}}));
			}};// @formatter:on
		return json;
	}
}
