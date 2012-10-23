package org.culturegraph.cluster.job.convert;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.culturegraph.cluster.job.convert.ResolveGndUrisInLobidNTriples.ResolveTriplesMapper;
import org.culturegraph.cluster.job.convert.ResolveGndUrisInLobidNTriples.ResolveTriplesReducer;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the {@link ResolveGndUrisInLobidNTriples} class.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class ResolveGndUrisInLobidNTriplesTest {

	private static final String TRIPLE_1 =
			"<http://lobid.org/resource/HT000000716> "
					+ "<http://purl.org/dc/elements/1.1/creator>"
					+ "<http://d-nb.info/gnd/118643606>.";
	private static final String TRIPLE_2 =
			"<http://d-nb.info/gnd/118643606> "
					+ "<http://d-nb.info/standards/elementset/gnd#preferredNameForThePerson>"
					+ "\"Adamucci, Antonio\".";
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		final ResolveTriplesMapper mapper = new ResolveTriplesMapper();
		final ResolveTriplesReducer reducer = new ResolveTriplesReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testMapper() throws IOException { // NOPMD (MRUnit, no explicit
													// assertion)
		mapDriver.addInput(new LongWritable(), new Text(TRIPLE_1));
		mapDriver.addInput(new LongWritable(), new Text(TRIPLE_2));
		mapDriver.addOutput(new Text("<http://d-nb.info/gnd/118643606>"),
				new Text(TRIPLE_1));
		mapDriver.addOutput(new Text("<http://d-nb.info/gnd/118643606>"),
				new Text(TRIPLE_2));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException { // NOPMD (MRUnit, no explicit
													// assertion)
		reduceDriver.addInput(new Text("<http://d-nb.info/gnd/118643606>"),
				Arrays.asList(new Text(TRIPLE_1), new Text(TRIPLE_2)));
		reduceDriver.addOutput(new Text(
				"<http://lobid.org/resource/HT000000716>"), new Text(
				"<http://purl.org/dc/elements/1.1/creator#preferredNameForThePerson>"
						+ "\"Adamucci, Antonio\"."));
		reduceDriver.addOutput(new Text(
				"<http://lobid.org/resource/HT000000716>"), new Text(
				"<http://purl.org/dc/elements/1.1/creator>"
						+ "<http://d-nb.info/gnd/118643606>."));
		reduceDriver.runTest();
	}

}
