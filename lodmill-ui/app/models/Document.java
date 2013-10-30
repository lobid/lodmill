/* Copyright 2012-2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.json.simple.JSONValue;
import org.lobid.lodmill.JsonLdConverter;
import org.lobid.lodmill.JsonLdConverter.Format;

import play.Logger;
import play.Play;
import play.libs.Json;

import com.fasterxml.jackson.databind.JsonNode;
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
	private Index index;
	private String field;

	/** @return The document ID. */
	public String getId() {
		return id;
	}

	/**
	 * @return The JSON source for this document as compact JSON-LD with an
	 *         extracted, external context, or null if conversion failed.
	 */
	public String getSource() {
		try {
			final Pair<URL, String> localAndPublicContextUrls = getContextUrls();
			final Map<String, Object> contextObject =
					(Map<String, Object>) JSONUtils.fromURL(localAndPublicContextUrls
							.getLeft());
			final Map<String, Object> compactJsonLd =
					(Map<String, Object>) JSONLD.compact(JSONUtils.fromString(source),
							contextObject);
			compactJsonLd.put("@context", localAndPublicContextUrls.getRight());
			final String result = JSONUtils.toString(compactJsonLd);
			return this.field.isEmpty() ? result : findField(result);
		} catch (JSONLDProcessingError | IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	private String findField(final String result) {
		final List<JsonNode> fieldValues = Json.parse(result).findValues(field);
		final JsonNode node =
				fieldValues.size() == 1 && fieldValues.get(0).isArray() ? /**/
				Json.toJson(fieldValues.get(0)) : Json.toJson(fieldValues);
		return Json.stringify(node);
	}

	/**
	 * @return The JSON source for this document as compact JSON-LD with full
	 *         properties (i.e. without a context), or null if conversion failed.
	 */
	public String getSourceWithFullProperties() {
		try {
			final Object compactJsonLd =
					JSONLD.compact(JSONUtils.fromString(source),
							new HashMap<String, Object>());
			return JSONUtils.toString(compactJsonLd);
		} catch (JSONLDProcessingError | IOException e) {
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
	 * @param index The index that this document is part of
	 * @param field The field to consider as this document's data (if empty, the
	 *          complete source will be the document's content)
	 */
	public Document(final String id, final String source, final Index index,
			final String field) { // NOPMD
		this.id = id;
		this.source = source;
		this.index = index;
		this.field = field;
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

	private Pair<URL, String> getContextUrls() throws MalformedURLException {
		final String path = "public/contexts";
		final String file = index.id() + ".json";
		URL localContextResourceUrl =
				Play.application().resource("/" + path + "/" + file);
		if (localContextResourceUrl == null) // no app running, use plain local file
			localContextResourceUrl = new File(path, file).toURI().toURL();
		final String publicContextUrl =
				"http://api.lobid.org"
						+ controllers.routes.Assets.at("/" + path, file).url();
		return new ImmutablePair<>(localContextResourceUrl, publicContextUrl);
	}

}
