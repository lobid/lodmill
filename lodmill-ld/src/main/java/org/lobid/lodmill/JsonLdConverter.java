/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.StringReader;
import java.io.StringWriter;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.culturegraph.semanticweb.sink.AbstractModelWriter.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import de.dfki.km.json.JSONUtils;
import de.dfki.km.json.jsonld.JSONLDProcessor;
import de.dfki.km.json.jsonld.impl.JenaJSONLDSerializer;
import de.dfki.km.json.jsonld.impl.JenaTripleCallback;

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

	public JsonLdConverter(final Format format) {
		this.format = format;
	}

	public String toRdf(final String jsonLd) {
		try {
			final JSONLDProcessor processor = new JSONLDProcessor();
			final JenaTripleCallback callback = new JenaTripleCallback();
			processor.triples(JSONUtils.fromString(jsonLd), callback);
			final StringWriter writer = new StringWriter();
			callback.getJenaModel().write(writer, format.getName());
			return writer.toString();
		} catch (JsonParseException | JsonMappingException e) {
			LOG.error(e.getMessage(), e);
		}
		return null;
	}

	public String toJsonLd(final String rdf) {
		final Model model = ModelFactory.createDefaultModel();
		model.read(new StringReader(rdf), null, format.getName());
		final JenaJSONLDSerializer serializer = new JenaJSONLDSerializer();
		serializer.importModel(model);
		return JSONUtils.toString(serializer.asObject());
	}

}
