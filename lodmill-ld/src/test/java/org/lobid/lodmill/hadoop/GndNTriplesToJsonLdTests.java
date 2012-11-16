/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import static org.lobid.lodmill.hadoop.LobidNTriplesToJsonLdTests.indexMap;
import static org.lobid.lodmill.hadoop.ResolveObjectUrisInLobidNTriplesTests.GND_CREATOR_ID;
import static org.lobid.lodmill.hadoop.ResolveObjectUrisInLobidNTriplesTests.GND_TRIPLE_1;
import static org.lobid.lodmill.hadoop.ResolveObjectUrisInLobidNTriplesTests.GND_TRIPLE_2;
import static org.lobid.lodmill.hadoop.ResolveObjectUrisInLobidNTriplesTests.GND_TRIPLE_3;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
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
public final class GndNTriplesToJsonLdTests {

	private static final String TRIPLE_ID = "http://d-nb.info/gnd/"
			+ GND_CREATOR_ID;
	private static final String TRIPLE_URI = "<" + TRIPLE_ID + ">";
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
		mapDriver.setConfiguration(configuration);
		reduceDriver.setConfiguration(configuration);
	}

	@Test
	public void testMapper() throws IOException { // NOPMD (MRUnit, no explicit
													// assertion)
		mapDriver.addInput(new LongWritable(), new Text(GND_TRIPLE_1));
		mapDriver.addInput(new LongWritable(), new Text(GND_TRIPLE_2));
		mapDriver.addInput(new LongWritable(), new Text(GND_TRIPLE_3));
		mapDriver.withOutput(new Text(TRIPLE_URI), new Text(GND_TRIPLE_1))
				.withOutput(new Text(TRIPLE_URI), new Text(GND_TRIPLE_2))
				.withOutput(new Text(TRIPLE_URI), new Text(GND_TRIPLE_3));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException { // NOPMD (MRUnit, no explicit
													// assertion)
		reduceDriver.withInput(new Text(TRIPLE_URI), Arrays.asList(new Text(
				GND_TRIPLE_1), new Text(GND_TRIPLE_2), new Text(GND_TRIPLE_3)));
		reduceDriver.withOutput(
				new Text(JSONValue
						.toJSONString(indexMap(INDEX, TYPE, TRIPLE_ID))),
				new Text(JSONValue.toJSONString(jsonMap())));
		reduceDriver.runTest();
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
			put("http://d-nb.info/standards/elementset/gnd#preferredNameForThePerson",
					"Adamucci, Antonio");
			put("http://d-nb.info/standards/elementset/gnd#dateOfDeath", "1885");
			put("http://d-nb.info/standards/elementset/gnd#dateOfBirth", "1828");
		}};// @formatter:on
		return json;
	}
}
