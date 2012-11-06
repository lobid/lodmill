/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.lobid.lodmill.hadoop.ResolveGndUrisInLobidNTriples.ResolveTriplesMapper;
import org.lobid.lodmill.hadoop.ResolveGndUrisInLobidNTriples.ResolveTriplesReducer;

/**
 * Test the {@link ResolveGndUrisInLobidNTriples} class.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class ResolveGndUrisInLobidNTriplesTests {

	private static final String GND_CREATOR_ID = "118643606";
	private static final String LOBID_TRIPLE =
			"<http://lobid.org/resource/HT000000716> "
					+ "<http://purl.org/dc/elements/1.1/creator>"
					+ "<http://d-nb.info/gnd/118643606>.";
	private static final String GND_TRIPLE_1 = gnd("preferredNameForThePerson",
			"Adamucci, Antonio");
	private static final String GND_TRIPLE_2 = gnd("dateOfBirth", "1828");
	private static final String GND_TRIPLE_3 = gnd("dateOfDeath", "1885");

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		final ResolveTriplesMapper mapper = new ResolveTriplesMapper();
		final ResolveTriplesReducer reducer = new ResolveTriplesReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	private static String gnd(String predicate, String literal) {
		return String.format("<http://d-nb.info/gnd/%s> "
				+ "<http://d-nb.info/standards/elementset/gnd#%s>" + "\"%s\".",
				GND_CREATOR_ID, predicate, literal);
	}

	@Test
	public void testProperties() throws IOException {
		assertEquals("3 predicates", 3,
				ResolveGndUrisInLobidNTriples.PREDICATES.size());
		assertEquals("3 fsl-paths", 3,
				ResolveGndUrisInLobidNTriples.FSL_PATHS.size());
	}

	@Test
	public void testMapper() throws IOException { // NOPMD (MRUnit, no explicit
													// assertion)
		mapDriver.addInput(new LongWritable(), new Text(LOBID_TRIPLE));
		mapDriver.addInput(new LongWritable(), new Text(GND_TRIPLE_1));
		mapDriver.addInput(new LongWritable(), new Text(GND_TRIPLE_2));
		mapDriver.addInput(new LongWritable(), new Text(GND_TRIPLE_3));
		String key = "<http://d-nb.info/gnd/118643606>";
		mapDriver.addOutput(new Text(key), new Text(LOBID_TRIPLE));
		mapDriver.addOutput(new Text(key), new Text(GND_TRIPLE_1));
		mapDriver.addOutput(new Text(key), new Text(GND_TRIPLE_2));
		mapDriver.addOutput(new Text(key), new Text(GND_TRIPLE_3));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException { // NOPMD (MRUnit, no explicit
													// assertion)
		reduceDriver.addInput(new Text("<http://d-nb.info/gnd/118643606>"),
				Arrays.asList(new Text(LOBID_TRIPLE), new Text(GND_TRIPLE_1),
						new Text(GND_TRIPLE_2), new Text(GND_TRIPLE_3)));
		reduceDriver.addOutput(new Text(
				"<http://lobid.org/resource/HT000000716>"), new Text(
				"<http://purl.org/dc/elements/1.1/creator#preferredNameForThePerson>"
						+ "\"Adamucci, Antonio\"."));
		reduceDriver.addOutput(new Text(
				"<http://lobid.org/resource/HT000000716>"), new Text(
				"<http://purl.org/dc/elements/1.1/creator#dateOfDeath>"
						+ "\"1885\"."));
		reduceDriver.addOutput(new Text(
				"<http://lobid.org/resource/HT000000716>"), new Text(
				"<http://purl.org/dc/elements/1.1/creator#dateOfBirth>"
						+ "\"1828\"."));
		reduceDriver.addOutput(new Text(
				"<http://lobid.org/resource/HT000000716>"), new Text(
				"<http://purl.org/dc/elements/1.1/creator>"
						+ "<http://d-nb.info/gnd/118643606>."));
		reduceDriver.runTest();
	}

}
