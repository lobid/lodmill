/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.TestDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Before;
import org.junit.Test;
import org.lobid.lodmill.hadoop.NTriplesToJsonLd.NTriplesToJsonLdMapper;
import org.lobid.lodmill.hadoop.NTriplesToJsonLd.NTriplesToJsonLdReducer;

import com.google.common.collect.ImmutableMap;

/**
 * Test the {@link NTriplesToJsonLd} class.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public final class UnitTestLobidNTriplesToJsonLd {

	private static final String TRIPLE_ID =
			"http://lobid.org/resource/HT000000716";
	private static final String TRIPLE_URI = "<" + TRIPLE_ID + ">";
	private static final String TRIPLE_1 = TRIPLE_URI
			+ "<http://purl.org/dc/terms/creator>"
			+ "<http://d-nb.info/gnd/118643606>.";
	private static final String TRIPLE_2 = TRIPLE_URI
			+ "<http://purl.org/dc/elements/1.1/creator>" + "\"Adamucci, Antonio\".";
	private static final String TRIPLE_3 = TRIPLE_URI
			+ " <http://purl.org/dc/terms/subject>"
			/* Some N-Triples contain (malformed) URIs as literals: */
			+ " \" https://dewey.info/class/[892.1, 22]/\".";
	private static final String TRIPLE_4 = TRIPLE_URI
			+ "<http://purl.org/dc/terms/subject#prefLabel>"
			+ "\"International migration & colonization@en\".";
	private static final String INDEX = "lobid-index";
	private static final String TYPE = "json-ld-lobid";
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		final NTriplesToJsonLdMapper mapper = new NTriplesToJsonLdMapper();
		final NTriplesToJsonLdReducer reducer = new NTriplesToJsonLdReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		setConfiguration(mapDriver);
		setConfiguration(reduceDriver);
	}

	private static void setConfiguration(TestDriver<?, ?, ?, ?, ?> driver) {
		driver.getConfiguration().set(NTriplesToJsonLd.INDEX_NAME, INDEX);
		driver.getConfiguration().set(NTriplesToJsonLd.INDEX_TYPE, TYPE);
	}

	@Test
	public void testMapper() throws IOException { // NOPMD (MRUnit, no explicit
		// assertion)
		mapDriver.addInput(new LongWritable(), new Text(TRIPLE_1));
		mapDriver.addInput(new LongWritable(), new Text(TRIPLE_2));
		mapDriver.addInput(new LongWritable(), new Text(TRIPLE_3));
		mapDriver.addInput(new LongWritable(), new Text(TRIPLE_4));
		mapDriver.withOutput(new Text(TRIPLE_URI), new Text(TRIPLE_1))
				.withOutput(new Text(TRIPLE_URI), new Text(TRIPLE_2))
				.withOutput(new Text(TRIPLE_URI), new Text(TRIPLE_3))
				.withOutput(new Text(TRIPLE_URI), new Text(TRIPLE_4));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException { // NOPMD (MRUnit, no explicit
		// assertion)
		reduceDriver.withInput(new Text(TRIPLE_URI), Arrays.asList(new Text(
				TRIPLE_1), new Text(TRIPLE_2), new Text(TRIPLE_3), new Text(TRIPLE_4)));
		reduceDriver.withOutput(
				new Text(JSONValue.toJSONString(indexMap(INDEX, TYPE, TRIPLE_ID))),
				new Text(JSONValue.toJSONString(correctJson())));
		reduceDriver.runTest();
	}

	static Map<String, Map<?, ?>> indexMap(final String indexName,
			final String indexType, final String resourceId) {
		final Map<String, String> map = new HashMap<>();
		map.put("_index", indexName);
		map.put("_type", indexType);
		map.put("_id", resourceId);
		final Map<String, Map<?, ?>> index = new HashMap<>();
		index.put("index", map);
		return index;
	}

	@SuppressWarnings({ "unchecked" })
	static JSONObject correctJson() {
		JSONArray array = new JSONArray();
		JSONObject obj = new JSONObject();
		obj.put("@id", TRIPLE_ID);
		obj.put("http://purl.org/dc/terms/subject#prefLabel", Arrays
				.asList(ImmutableMap.of("@value",
						"International migration & colonization@en")));
		obj.put("http://purl.org/dc/terms/subject", Arrays.asList(ImmutableMap.of(
				"@id", "https://dewey.info/class/[892.1, 22]/")));
		obj.put("http://purl.org/dc/elements/1.1/creator",
				Arrays.asList(ImmutableMap.of("@value", "Adamucci, Antonio")));
		obj.put("http://purl.org/dc/terms/creator",
				Arrays.asList(ImmutableMap.of("@id", "http://d-nb.info/gnd/118643606")));
		array.add(obj);
		return new JSONObject(ImmutableMap.of("@graph", array));
	}
}
