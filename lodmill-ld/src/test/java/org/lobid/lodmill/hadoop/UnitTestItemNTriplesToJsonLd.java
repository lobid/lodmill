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
 * Test the {@link NTriplesToJsonLd} class with a lobid item.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public final class UnitTestItemNTriplesToJsonLd {

	private static final String TRIPLE_ID =
			"http://lobid.org/item/BT000000079:GA+644";
	private static final String PARENT_ID =
			"http://lobid.org/resource/BT000000079";
	private static final String TRIPLE_URI = "<" + TRIPLE_ID + ">";
	private static final String TRIPLE_1 = TRIPLE_URI
			+ "<http://purl.org/vocab/frbr/core#owner>"
			+ "<http://lobid.org/organisation/DE-Sol1>.";
	private static final String TRIPLE_2 = TRIPLE_URI
			+ "<http://purl.org/vocab/frbr/core#exemplarOf>" + "<" + PARENT_ID + ">.";
	private static final String INDEX = "lobid-index";
	private static final String TYPE = "json-ld-lobid-item";
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
	public void testMapper() throws IOException {
		mapDriver.addInput(new LongWritable(), new Text(TRIPLE_1));
		mapDriver.addInput(new LongWritable(), new Text(TRIPLE_2));
		mapDriver.withOutput(new Text(TRIPLE_URI), new Text(TRIPLE_1)).withOutput(
				new Text(TRIPLE_URI), new Text(TRIPLE_2));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		reduceDriver.withInput(new Text(TRIPLE_URI),
				Arrays.asList(new Text(TRIPLE_1), new Text(TRIPLE_2)));
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
		map.put("_parent", PARENT_ID);
		final Map<String, Map<?, ?>> index = new HashMap<>();
		index.put("index", map);
		return index;
	}

	@SuppressWarnings({ "unchecked" })
	static JSONObject correctJson() {
		JSONArray array = new JSONArray();
		JSONObject obj = new JSONObject();
		obj.put("@id", TRIPLE_ID);
		obj.put("http://purl.org/vocab/frbr/core#owner", Arrays.asList(ImmutableMap
				.of("@id", "http://lobid.org/organisation/DE-Sol1")));
		obj.put("http://purl.org/vocab/frbr/core#exemplarOf",
				Arrays.asList(ImmutableMap.of("@id", PARENT_ID)));
		array.add(obj);
		return new JSONObject(ImmutableMap.of("@graph", array));
	}
}
