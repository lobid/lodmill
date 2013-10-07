/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.json.simple.JSONValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.lobid.lodmill.JsonLdConverter;
import org.lobid.lodmill.JsonLdConverter.Format;

/**
 * Test the {@link JsonLdConverter} class.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@RunWith(value = Parameterized.class)
public final class UnitTestJsonLdConverter {

	private final Format format;

	/**
	 * @return The data to use for this parameterized test (test is executed once
	 *         for every element, which is passed to the constructor of this test)
	 */
	@Parameters
	public static Collection<Object[]> data() {
		final Object[][] tests =
				{ { Format.N3 }, { Format.N_TRIPLE }, { Format.RDF_XML },
						{ Format.RDF_XML_ABBREV }, { Format.TURTLE } };
		return Arrays.asList(tests);
	}

	/**
	 * @param format The Format to use for this test (passed from {@link #data()})
	 */
	public UnitTestJsonLdConverter(final Format format) {
		this.format = format;
		System.out.println("Testing conversion with: " + format);
	}

	/* TODO: when Jena serializes, it rightfully complains about these: */
	final String jsonLdSample = JSONValue.toJSONString(
			UnitTestLobidNTriplesToJsonLd.correctJson()).replace(
			"https:\\/\\/dewey.info\\/class\\/[892.1, 22]\\/",
			"https:\\/\\/dewey.info\\/class\\/892.1\\/");

	@SuppressWarnings("javadoc")
	@Test
	public void testConversion() {
		final JsonLdConverter converter = new JsonLdConverter(format);
		final String rdf1 = converter.toRdf(jsonLdSample);
		final String json = converter.toJsonLd(rdf1);
		final String rdf2 = converter.toRdf(json);
		assertEquals(
				"Original and round-tripped RDF serialization should be equal", rdf1,
				rdf2);
	}
}
