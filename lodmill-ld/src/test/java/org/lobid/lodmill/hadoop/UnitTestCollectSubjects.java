/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.lobid.lodmill.hadoop.CollectSubjects.CollectSubjectsMapper;
import org.lobid.lodmill.hadoop.CollectSubjects.CollectSubjectsReducer;

/**
 * Test the {@link CollectSubjects} class.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public final class UnitTestCollectSubjects {

	static final String GND_CREATOR_ID = "118643606";
	private static final String GND_ID = "http://d-nb.info/gnd/" + GND_CREATOR_ID;
	private static final String LOBID_ID_GND =
			"http://lobid.org/resource/HT000000716";
	private static final String LOBID_TRIPLE_GND = "<" + LOBID_ID_GND + "> "
			+ "<http://purl.org/dc/terms/creator>" + "<" + GND_ID + ">.";
	private static final String LOBID_ID_DEWEY =
			"http://lobid.org/resource/HT007307035";
	private static final String DEWEY_ID_PLAIN = "http://dewey.info/class/325/";
	private static final String DEWEY_ID_SUFFIXED = DEWEY_ID_PLAIN
			+ "2009/08/about.en";

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		final CollectSubjectsMapper mapper = new CollectSubjectsMapper();
		final CollectSubjectsReducer reducer = new CollectSubjectsReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	static String gnd(final String predicate, final String literal) {
		return String.format("<http://d-nb.info/gnd/%s> "
				+ "<http://d-nb.info/standards/elementset/gnd#%s>" + "\"%s\".",
				GND_CREATOR_ID, predicate, literal);
	}

	@SuppressWarnings("static-method")
	@Test
	public void testProperties() {
		assertEquals("number of entries to resolve", 11,
				CollectSubjects.TO_RESOLVE.size());
		assertEquals("number of predicates", 18, CollectSubjects.PREDICATES.size());
		assertEquals("number of parents", 1, CollectSubjects.PARENTS.size());
	}

	@Test
	public void testMapperGnd() throws IOException { // NOPMD (MRUnit)
		mapDriver.addInput(new LongWritable(), new Text(LOBID_TRIPLE_GND));
		mapDriver.addInput(new LongWritable(),
				new Text(gnd("preferredNameForThePerson", "Adamucci, Antonio")));
		mapDriver.addOutput(new Text(GND_ID), new Text(LOBID_ID_GND));
		mapDriver.runTest();
	}

	@Test
	public void testMapperDewey() throws IOException { // NOPMD (MRUnit)
		mapDriver
				.addInput(new LongWritable(), new Text("<" + LOBID_ID_DEWEY + "> "
						+ "<http://purl.org/dc/terms/subject> " + "<" + DEWEY_ID_PLAIN
						+ "> ."));
		mapDriver.addInput(new LongWritable(), new Text("<" + DEWEY_ID_SUFFIXED
				+ "> " + "<http://www.w3.org/2004/02/skos/core#prefLabel> "
				+ "\"International migration & colonization\"@en ."));
		mapDriver.addOutput(new Text(DEWEY_ID_SUFFIXED), new Text(LOBID_ID_DEWEY));
		mapDriver.runTest();
	}

	@Test
	public void testReducerGnd() throws IOException { // NOPMD (MRUnit)
		reduceDriver.addInput(new Text(GND_ID),
				Arrays.asList(new Text(LOBID_ID_GND), new Text(LOBID_ID_DEWEY)));
		reduceDriver.addOutput(new Text(GND_ID), new Text(LOBID_ID_GND + ","
				+ LOBID_ID_DEWEY));
		reduceDriver.runTest();
	}

	@Test
	public void testReducerDewey() throws IOException { // NOPMD (MRUnit)
		reduceDriver.addInput(new Text(DEWEY_ID_SUFFIXED),
				Arrays.asList(new Text(LOBID_ID_GND), new Text(LOBID_ID_DEWEY)));
		reduceDriver.addOutput(new Text(DEWEY_ID_SUFFIXED), new Text(LOBID_ID_GND
				+ "," + LOBID_ID_DEWEY));
		reduceDriver.runTest();
	}

	private enum BlankGeo {
		/*@formatter:off*/
		LOBID_1("<http://lobid.org/organisation/AF-KaIS> "
				+ "<http://www.w3.org/2003/01/geo/wgs84_pos#location> _:node16vicghfdx21 ."),
		LOBID_2("<http://lobid.org/organisation/AE-ShAU> "
						+ "<http://www.w3.org/2003/01/geo/wgs84_pos#location> _:node16vicghfdx21 ."),
		BLANK("_:node16vicghfdx21 "
				+ "<http://www.w3.org/2003/01/geo/wgs84_pos#lat> \"-25.6494315\" .");
		/*@formatter:on*/
		final String triple;

		BlankGeo(String triple) {
			this.triple = triple;
		}
	}

	@Test
	public void testMapperBlanksGeo() throws IOException {
		final List<Pair<Text, Text>> result = runGeoMapDriver();
		assertEquals(new Text("_:node16vicghfdx21:somefile"), result.get(0)
				.getFirst());
		assertEquals(new Text("http://lobid.org/organisation/AF-KaIS"),
				result.get(0).getSecond());
	}

	private List<Pair<Text, Text>> runGeoMapDriver() throws IOException {
		mapDriver.addInput(new LongWritable(), new Text(BlankGeo.LOBID_1.triple));
		mapDriver.addInput(new LongWritable(), new Text(BlankGeo.LOBID_2.triple));
		mapDriver.addInput(new LongWritable(), new Text(BlankGeo.BLANK.triple));
		return mapDriver.run();
	}

	@Test
	public void testReducerBlanksGeo() throws IOException {// NOPMD (MRUnit)
		setUpReduceInput(runGeoMapDriver());
		reduceDriver.addOutput(new Text("_:node16vicghfdx21:somefile"), new Text(
				"http://lobid.org/organisation/AF-KaIS,"
						+ "http://lobid.org/organisation/AE-ShAU"));
		reduceDriver.runTest(false);
	}

	private enum BlankAddress {
		/*@formatter:off*/
		LOBID_1("<http://lobid.org/organisation/AE-ShAU> "
				+ "<http://www.w3.org/2006/vcard/ns#adr> _:node16vicghfdx20 ."),
		LOBID_2("<http://lobid.org/organisation/AF-KaIS> "
						+ "<http://www.w3.org/2006/vcard/ns#adr> _:node16vicghfdx20 ."),
		BLANK("_:node16vicghfdx20 "
				+ "<http://www.w3.org/2006/vcard/ns#country-name> \"United Arab Emirates\" .");
		/*@formatter:on*/
		final String triple;

		BlankAddress(String triple) {
			this.triple = triple;
		}
	}

	@Test
	public void testMapperBlanksAddress() throws IOException {
		final List<Pair<Text, Text>> result = runAddressMapDriver();
		assertEquals(new Text("_:node16vicghfdx20:somefile"), result.get(0)
				.getFirst());
		assertEquals(new Text("http://lobid.org/organisation/AE-ShAU"),
				result.get(0).getSecond());
	}

	private List<Pair<Text, Text>> runAddressMapDriver() throws IOException {
		for (BlankAddress elem : BlankAddress.values()) {
			mapDriver.addInput(new LongWritable(), new Text(elem.triple));
		}
		return mapDriver.run();
	}

	@Test
	public void testReducerBlanksAddress() throws IOException { // NOPMD (MRUnit)
		setUpReduceInput(runAddressMapDriver());
		reduceDriver.addOutput(new Text("_:node16vicghfdx20:somefile"), new Text(
				"http://lobid.org/organisation/AE-ShAU,"
						+ "http://lobid.org/organisation/AF-KaIS"));
		reduceDriver.runTest(false);
	}

	private void setUpReduceInput(final List<Pair<Text, Text>> mapResult) {
		final List<Text> values = new ArrayList<>();
		for (Pair<Text, Text> pair : mapResult) {
			values.add(pair.getSecond());
		}
		reduceDriver.addInput(mapResult.get(0).getFirst(), values);
	}
}
