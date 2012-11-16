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
import org.lobid.lodmill.hadoop.ResolveObjectUrisInLobidNTriples.ResolveTriplesMapper;
import org.lobid.lodmill.hadoop.ResolveObjectUrisInLobidNTriples.ResolveTriplesReducer;

/**
 * Test the {@link ResolveObjectUrisInLobidNTriples} class.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class ResolveObjectUrisInLobidNTriplesTests {

	static final String GND_CREATOR_ID = "118643606";
	private static final String LOBID_TRIPLE_1 =
			"<http://lobid.org/resource/HT000000716> "
					+ "<http://purl.org/dc/elements/1.1/creator>"
					+ "<http://d-nb.info/gnd/118643606>.";
	static final String GND_TRIPLE_1 = gnd("preferredNameForThePerson",
			"Adamucci, Antonio");
	static final String GND_TRIPLE_2 = gnd("dateOfBirth", "1828");
	static final String GND_TRIPLE_3 = gnd("dateOfDeath", "1885");
	private static final String LOBID_DEWEY_TRIPLE =
			"<http://lobid.org/resource/HT007307035> "
					+ "<http://purl.org/dc/terms/subject> "
					+ "<http://dewey.info/class/325/> .";
	private static final String LOBID_DEWEY_TRIPLE_SUFFIXED =
			"<http://lobid.org/resource/HT007307035> "
					+ "<http://purl.org/dc/terms/subject> "
					+ "<http://dewey.info/class/325/2009/08/about.en> .";
	private static final String DEWEY_TRIPLE =
			"<http://dewey.info/class/325/2009/08/about.en> "
					+ "<http://www.w3.org/2004/02/skos/core#prefLabel> "
					+ "\"International migration & colonization\"@en .";

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		final ResolveTriplesMapper mapper = new ResolveTriplesMapper();
		final ResolveTriplesReducer reducer = new ResolveTriplesReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	private static String gnd(final String predicate, final String literal) {
		return String.format("<http://d-nb.info/gnd/%s> "
				+ "<http://d-nb.info/standards/elementset/gnd#%s>" + "\"%s\".",
				GND_CREATOR_ID, predicate, literal);
	}

	@Test
	public void testProperties() throws IOException {
		assertEquals("number of predicates", 4,
				ResolveObjectUrisInLobidNTriples.PREDICATES.size());
		assertEquals("number of fsl-paths", 4,
				ResolveObjectUrisInLobidNTriples.FSL_PATHS.size());
	}

	@Test
	public void testMapperGnd() throws IOException { // NOPMD
		// (MRUnit, no explicit assertion)
		mapDriver.addInput(new LongWritable(), new Text(LOBID_TRIPLE_1));
		mapDriver.addInput(new LongWritable(), new Text(GND_TRIPLE_1));
		mapDriver.addInput(new LongWritable(), new Text(GND_TRIPLE_2));
		mapDriver.addInput(new LongWritable(), new Text(GND_TRIPLE_3));
		final String key = "<http://d-nb.info/gnd/118643606>";
		mapDriver.addOutput(new Text(key), new Text(LOBID_TRIPLE_1));
		mapDriver.addOutput(new Text(key), new Text(GND_TRIPLE_1));
		mapDriver.addOutput(new Text(key), new Text(GND_TRIPLE_2));
		mapDriver.addOutput(new Text(key), new Text(GND_TRIPLE_3));
		mapDriver.runTest();
	}

	@Test
	public void testMapperDewey() throws IOException { // NOPMD
		// (MRUnit, no explicit assertion)
		mapDriver.addInput(new LongWritable(), new Text(LOBID_DEWEY_TRIPLE));
		mapDriver.addInput(new LongWritable(), new Text(DEWEY_TRIPLE));
		final String deweySubject =
				"<http://dewey.info/class/325/2009/08/about.en>";
		mapDriver.addOutput(new Text(deweySubject), new Text(
				LOBID_DEWEY_TRIPLE_SUFFIXED));
		mapDriver.addOutput(new Text(deweySubject), new Text(DEWEY_TRIPLE));
		mapDriver.runTest();
	}

	@Test
	public void testReducerGnd() throws IOException { // NOPMD
		// (MRUnit, no explicit assertion)
		reduceDriver.addInput(new Text("<http://d-nb.info/gnd/118643606>"),
				Arrays.asList(new Text(LOBID_TRIPLE_1), new Text(GND_TRIPLE_1),
						new Text(GND_TRIPLE_2), new Text(GND_TRIPLE_3)));
		final String lobidResource = "<http://lobid.org/resource/HT000000716>";
		reduceDriver.addOutput(new Text(lobidResource), new Text(
				"<http://purl.org/dc/elements/1.1/creator#preferredNameForThePerson>"
						+ "\"Adamucci, Antonio\"."));
		reduceDriver.addOutput(new Text(lobidResource), new Text(
				"<http://purl.org/dc/elements/1.1/creator#dateOfDeath>"
						+ "\"1885\"."));
		reduceDriver.addOutput(new Text(lobidResource), new Text(
				"<http://purl.org/dc/elements/1.1/creator#dateOfBirth>"
						+ "\"1828\"."));
		reduceDriver.addOutput(new Text(lobidResource), new Text(
				"<http://purl.org/dc/elements/1.1/creator>"
						+ "<http://d-nb.info/gnd/118643606>."));
		reduceDriver.runTest();
	}

	@Test
	public void testReducerDewey() throws IOException { // NOPMD
		// (MRUnit, no explicit assertion)
		reduceDriver.addInput(new Text(
				"<http://dewey.info/class/325/2009/08/about.en>"), Arrays
				.asList(new Text(LOBID_DEWEY_TRIPLE_SUFFIXED), new Text(
						DEWEY_TRIPLE)));
		reduceDriver.addOutput(new Text(
				"<http://lobid.org/resource/HT007307035>"), new Text(
				"<http://purl.org/dc/terms/subject#prefLabel>"
						+ "\"International migration & colonization@en\"."));
		reduceDriver.addOutput(new Text(
				"<http://lobid.org/resource/HT007307035>"), new Text(
				"<http://purl.org/dc/terms/subject>"
						+ "<http://dewey.info/class/325/2009/08/about.en>."));
		reduceDriver.runTest();
	}

}
