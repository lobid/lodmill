package org.culturegraph.cluster.job.convert;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.culturegraph.cluster.job.convert.NTriplesToJsonLd.NTriplesToJsonLdMapper;
import org.culturegraph.cluster.job.convert.NTriplesToJsonLd.NTriplesToJsonLdReducer;
import org.json.simple.JSONValue;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the {@link NTriplesToJsonLd} class.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class NTriplesToJsonLdTest {

	private static final String TRIPLE = "<http://lobid.org/resource/HT000000698> "
			+ "<http://xmlns.com/foaf/0.1/isPrimaryTopicOf> "
			+ "<http://193.30.112.134/F/?func=find-c&ccl_term=IDN%3DHT000000698> .";
	private static final String TRIPLE_URI = "<http://lobid.org/resource/HT000000698>";
	private static final String TRIPLE_ID = "http://lobid.org/resource/HT000000698";
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		final NTriplesToJsonLdMapper mapper = new NTriplesToJsonLdMapper();
		final NTriplesToJsonLdReducer reducer = new NTriplesToJsonLdReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testMapper() { // NOPMD (MRUnit, no explicit assertion)
		mapDriver.withInput(new LongWritable(), new Text(TRIPLE));
		mapDriver.withOutput(new Text(TRIPLE_URI), new Text(TRIPLE));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() { // NOPMD (MRUnit, no explicit assertion)
		reduceDriver.withInput(new Text(TRIPLE_URI),
				Arrays.asList(new Text(TRIPLE), new Text(TRIPLE)));
		reduceDriver.withOutput(new Text(JSONValue.toJSONString(indexMap())),
				new Text(JSONValue.toJSONString(jsonMap())));
		reduceDriver.runTest();
	}

	private Map<String, Map<?, ?>> indexMap() {
		final Map<String, String> map = new HashMap<String, String>();
		map.put("_index", "json-ld-index");
		map.put("_type", "json-ld");
		map.put("_id", TRIPLE_ID);
		final Map<String, Map<?, ?>> index = new HashMap<String, Map<?, ?>>();
		index.put("index", map);
		return index;
	}

	private Map<String, ?> jsonMap() {
		final Map<String, String> contextMap = new HashMap<String, String>();
		contextMap.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
		contextMap.put("xsd", "http://www.w3.org/2001/XMLSchema#");
		contextMap.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		final Map<String, String> topicMap = new HashMap<String, String>();
		final String idKey = "@id";
		topicMap.put(idKey,
				"http://193.30.112.134/F/?func=find-c&ccl_term=IDN%3DHT000000698");
		final Map<String, Object> json = new HashMap<String, Object>();
		json.put("@context", contextMap);
		json.put("http://xmlns.com/foaf/0.1/isPrimaryTopicOf", topicMap);
		json.put(idKey, TRIPLE_ID);
		return json;
	}
}