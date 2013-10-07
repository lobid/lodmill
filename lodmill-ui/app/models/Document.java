/* Copyright 2012-2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models;

import java.util.HashMap;

import org.json.simple.JSONValue;
import org.lobid.lodmill.JsonLdConverter;
import org.lobid.lodmill.JsonLdConverter.Format;

import play.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.github.jsonldjava.core.JSONLD;
import com.github.jsonldjava.core.JSONLDProcessingError;
import com.github.jsonldjava.utils.JSONUtils;
import com.hp.hpl.jena.shared.BadURIException;

/**
 * Documents returned from the ElasticSearch index.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class Document {

	transient String matchedField;
	private final String source;
	private transient String id; // NOPMD

	/** @return The document ID. */
	public String getId() {
		return id;
	}

	/** @return The JSON source for this document, or null. */
	public String getSource() {
		try {
			final Object compactJsonLd =
					JSONLD.compact(JSONUtils.fromString(source),
							new HashMap<String, Object>());
			return JSONUtils.toString(compactJsonLd);
		} catch (JsonParseException | JsonMappingException | JSONLDProcessingError e) {
			e.printStackTrace();
			return null;
		}
	}

	/** @return The field that matched the query. */
	public String getMatchedField() {
		return matchedField;
	}

	/**
	 * @param id The document ID
	 * @param source The document JSON source
	 */
	public Document(final String id, final String source) { // NOPMD
		this.id = id;
		this.source = source;
	}

	/**
	 * @param format The RDF serialization format to represent this document as
	 * @return This documents, in the given RDF format
	 */
	public String as(final Format format) { // NOPMD
		final JsonLdConverter converter = new JsonLdConverter(format);
		final String json = JSONValue.toJSONString(JSONValue.parse(source));
		String result = "";
		try {
			result = converter.toRdf(json);
		} catch (BadURIException x) {
			Logger.error(x.getMessage(), x);
		}
		return result;
	}
}
