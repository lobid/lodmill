/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
public final class NTriplesToJsonLdTests {

	private static final String TRIPLE_ID =
			"http://lobid.org/resource/HT000000716";
	private static final String TRIPLE_URI = "<" + TRIPLE_ID + ">";
	private static final String TRIPLE_1 = TRIPLE_URI
			+ "<http://purl.org/dc/elements/1.1/creator>"
			+ "<http://d-nb.info/gnd/118643606>.";
	private static final String TRIPLE_2 = TRIPLE_URI
			+ "<http://purl.org/dc/elements/1.1/creator>"
			+ "\"Adamucci, Antonio\".";
	private static final String TRIPLE_3 = TRIPLE_URI
			+ " <http://purl.org/dc/terms/subject>"
			/* Some N-Triples contain (malformed) URIs as literals: */
			+ " \" https://dewey.info/class/[892.1, 22]/\".";
	private static final String TRIPLE_4 = TRIPLE_URI
			+ "<http://purl.org/dc/terms/subject#prefLabel>"
			+ "\"International migration & colonization@en\".";
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
				TRIPLE_1), new Text(TRIPLE_2), new Text(TRIPLE_3), new Text(
				TRIPLE_4)));
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

	@SuppressWarnings("serial")
	/* using static init for better readability of nested result structure */
	static Map<String, ?> jsonMap() {
		final String idKey = "@id";// @formatter:off
		final Map<String, Object> json = new HashMap<String, Object>() {{//NOPMD
			put("@context", new HashMap<String, String>() {{//NOPMD
					put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
					put("xsd", "http://www.w3.org/2001/XMLSchema#");
					put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			}});
			put(idKey, TRIPLE_ID);
			put("http://purl.org/dc/elements/1.1/creator", Arrays.asList(
					"Adamucci, Antonio", // resolved literal
					new HashMap<String, String>() {{//NOPMD
							put(idKey, "http://d-nb.info/gnd/118643606");
					}}));
			put("http://purl.org/dc/terms/subject",
					new HashMap<String, String>() {{//NOPMD
							put(idKey, "https://dewey.info/class/[892.1, 22]/");
					}});
			put("http://purl.org/dc/terms/subject#prefLabel",
					"International migration & colonization@en");
		}};// @formatter:on
		return json;
	}
}
