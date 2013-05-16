/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package models;

import org.json.simple.JSONValue;
import org.lobid.lodmill.JsonLdConverter;
import org.lobid.lodmill.JsonLdConverter.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.shared.BadURIException;

/**
 * Documents returned from the ElasticSearch index.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class Document {

	private static final Logger LOG = LoggerFactory.getLogger(Document.class);
	transient String matchedField;
	private final String source;
	private transient String id; // NOPMD

	/** @return The document ID. */
	public String getId() {
		return id;
	}

	/** @return The JSON source for this document. */
	public String getSource() {
		return source;
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
			LOG.error(x.getMessage(), x);
		}
		return result;
	}
}
