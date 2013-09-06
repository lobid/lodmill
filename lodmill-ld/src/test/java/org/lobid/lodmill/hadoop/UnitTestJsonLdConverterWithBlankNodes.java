/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import org.json.JSONException;
import org.junit.Test;
import org.lobid.lodmill.JsonLdConverter;
import org.lobid.lodmill.JsonLdConverter.Format;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

/**
 * Test conversion to JSON-LD using the {@link JsonLdConverter} class.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class UnitTestJsonLdConverterWithBlankNodes {

	@SuppressWarnings({ "javadoc", "static-method" })
	@Test
	public void testConversion() throws FileNotFoundException, JSONException {
		final JsonLdConverter converter = new JsonLdConverter(Format.N_TRIPLE);
		final String rdfInput =
				load("src/test/resources/lobid-org-with-blank-nodes.nt");
		final String correctJson =
				load("src/test/resources/lobid-org-with-blank-nodes.json");
		final String generatedJson = /* skip invalid first line (for other tests) */
		converter.toJsonLd(rdfInput.substring(rdfInput.indexOf("\n") + 1));
		System.out.println(generatedJson);
		JSONAssert.assertEquals(removeBlankNodes(correctJson),
				removeBlankNodes(generatedJson), JSONCompareMode.LENIENT);
	}

	private static String removeBlankNodes(String string) {
		return string.replaceAll("_:t\\d", "");
	}

	private static String load(final String fileName)
			throws FileNotFoundException {
		try (Scanner scanner = new Scanner(new File(fileName))) {
			final StringBuilder builder = new StringBuilder();
			while (scanner.hasNextLine())
				builder.append(scanner.nextLine()).append("\n");
			return builder.toString().trim();
		}
	}
}
