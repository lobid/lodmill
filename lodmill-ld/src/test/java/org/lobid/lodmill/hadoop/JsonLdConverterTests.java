/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.culturegraph.semanticweb.sink.AbstractModelWriter.Format;
import org.json.simple.JSONValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.lobid.lodmill.JsonLdConverter;

/**
 * Test the {@link JsonLdConverter} class.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@RunWith(value = Parameterized.class)
public final class JsonLdConverterTests {

	private final Format format;

	@Parameters
	public static Collection<Object[]> data() {
		final Object[][] tests =
				{ { Format.N3 }, { Format.N_TRIPLE }, { Format.RDF_XML },
						{ Format.RDF_XML_ABBREV }, { Format.TURTLE } };
		return Arrays.asList(tests);
	}

	public JsonLdConverterTests(final Format format) {
		this.format = format;
		System.out.println("Testing conversion with: " + format);
	}

	/* TODO: when Jena serializes, it rightfully complains about these: */
	final String jsonLdSample = JSONValue.toJSONString(
			LobidNTriplesToJsonLdTests.jsonMap()).replace(
			"https:\\/\\/dewey.info\\/class\\/[892.1, 22]\\/",
			"https:\\/\\/dewey.info\\/class\\/892.1\\/");

	@Test
	public void testConversion() throws JsonParseException,
			JsonMappingException {
		final JsonLdConverter converter = new JsonLdConverter(format);
		final String rdf1 = converter.toRdf(jsonLdSample);
		final String json = converter.toJsonLd(rdf1);
		final String rdf2 = converter.toRdf(json);
		assertEquals(
				"Original and round-tripped RDF serialization should be equal",
				rdf1, rdf2);
	}
}
