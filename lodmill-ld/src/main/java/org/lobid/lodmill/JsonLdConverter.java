/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.jena.JenaRDFParser;
import com.github.jsonldjava.jena.JenaTripleCallback;
import com.github.jsonldjava.utils.JSONUtils;
import com.google.common.collect.ImmutableMap;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * A simple API for JSON-LD conversion (JSON-LD to RDF and RDF to JSON-LD),
 * wrapping calls to the Jena and jsonld-java libraries.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class JsonLdConverter {

	private static final Logger LOG = LoggerFactory
			.getLogger(JsonLdConverter.class);
	private final Format format;

	/**
	 * RDF serialization formats.
	 */
	@SuppressWarnings("javadoc")
	public static enum Format {
		RDF_XML("RDF/XML"), RDF_XML_ABBREV("RDF/XML-ABBREV"), N_TRIPLE("N-TRIPLE"), N3(
				"N3"), TURTLE("TURTLE");

		private final String name;

		Format(final String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

	/**
	 * @param format The RDF serialization format to use for conversions.
	 */
	public JsonLdConverter(final Format format) {
		this.format = format;
	}

	/**
	 * @param jsonLd The JSON-LD string to convert
	 * @return The input, converted to this converters RDF serialization, or null
	 */
	public String toRdf(final String jsonLd) {
		try {
			final Object jsonObject = JSONUtils.fromString(jsonLd);
			final JenaTripleCallback callback = new JenaTripleCallback();
			final Model model = (Model) JsonLdProcessor.toRDF(jsonObject, callback);
			final StringWriter writer = new StringWriter();
			model.write(writer, format.getName());
			return writer.toString();
		} catch (IOException | JsonLdError e) {
			LOG.error(e.getMessage(), e);
		}
		return null;
	}

	/**
	 * @param rdf The RDF string in this converter's format
	 * @return The input, converted to JSON-LD, or null
	 */
	public String toJsonLd(final String rdf) {
		final Model model = ModelFactory.createDefaultModel();
		model.read(new StringReader(rdf), null, format.getName());
		return jenaModelToJsonLd(model);
	}

	/**
	 * @param model The Jena model to serialize as a JSON-LD string
	 * @return The JSON-LD serialization of the Jena model, or null
	 */
	public static String jenaModelToJsonLd(final Model model) {
		final JenaRDFParser parser = new JenaRDFParser();
		try {
			Object json = JsonLdProcessor.fromRDF(model, new JsonLdOptions(), parser);
			/* We use the 'expanded' JSON-LD serialization for consistent field types: */
			json = JsonLdProcessor.expand(json);
			/* But we wrap it into a "@graph" for elasticsearch (still valid JSON-LD): */
			return JSONObject.toJSONString(ImmutableMap.of("@graph",
					(JSONArray) JSONValue.parse(JSONUtils.toString(json))));
		} catch (JsonLdError e) {
			e.printStackTrace();
		}
		return null;
	}
}
