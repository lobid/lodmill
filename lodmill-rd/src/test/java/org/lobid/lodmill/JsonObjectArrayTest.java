/* Copyright 2015  hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.jena.JenaTripleCallback;
import com.github.jsonldjava.utils.JSONUtils;
import com.hp.hpl.jena.rdf.model.Model;

/**
 * This is for API 2.0
 * 
 * @author Pascal Christoph (dr0i)
 * 
 */
@SuppressWarnings("javadoc")
public final class JsonObjectArrayTest {

	@SuppressWarnings("static-method")
	@Test
	public void testFlow() throws URISyntaxException {
		String TEST_FILENAME = "testObjectArrayPP.json";
		String json;
		try {
			json = FileUtils.readFileToString(new File(Thread.currentThread()
					.getContextClassLoader().getResource(TEST_FILENAME).toURI()));
			toRdf(json);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static String toRdf(String jsonLd) {
		try {
			Object jsonObject = JSONUtils.fromString(jsonLd);
			final JenaTripleCallback callback = new JenaTripleCallback();
			Model model = (Model) JsonLdProcessor.toRDF(jsonObject, callback);
			final StringWriter writer = new StringWriter();
			model.write(writer, "N-TRIPLE");
			System.out.println("jsonld to ntriples=" + writer.toString());
			System.out.println(
					"ntriples to json=" + JsonLdProcessor.fromRDF(writer.toString()));
			jsonObject = JsonLdProcessor.frame(jsonObject,
					JSONUtils.fromString(
							"{\"@context\": \"http://lobid.org/context/lobid-resources.json\"}"),
					new JsonLdOptions()).get("@graph");
			// TODO when using compact AND having about uris or created/modified, the
			// @graph is not missing
			// see also #433
			System.out
					.println("ntriples to json-framed=" + JSONUtils.toString(jsonObject));
			return writer.toString();
		} catch (IOException | JsonLdError e) {
			e.printStackTrace();
		}
		return null;
	}

}
