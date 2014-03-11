/* Copyright 2012-2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.lobid.lodmill.JsonLdConverter;
import org.lobid.lodmill.JsonLdConverter.Format;

import play.Logger;
import play.libs.Json;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.utils.JSONUtils;
import com.google.common.collect.ImmutableMap;
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
			final Pair<URL, String> localAndPublicContextUrls = index.context();
			final Map<String, Object> compactJsonLd =
					sourceAsCompactJsonLd((Map<String, Object>) JSONUtils
							.fromURL(localAndPublicContextUrls.getLeft()));
			compactJsonLd.put("@context", localAndPublicContextUrls.getRight());
			compactJsonLd.put("primaryTopic", id);
			final String result = JSONUtils.toString(compactJsonLd);
			return this.field.isEmpty() ? result : findField(result);
		} catch (JsonLdError | IOException e) {
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
			final Map<String, Object> jsonLd =
					sourceAsCompactJsonLd(new HashMap<String, Object>());
			jsonLd.put("http://xmlns.com/foaf/0.1/primaryTopic",
					ImmutableMap.of("@id", id));
			return JSONUtils.toString(jsonLd);
		} catch (JsonLdError | IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	private Map<String, Object> sourceAsCompactJsonLd(
			final Map<String, Object> contextObject) throws IOException,
			JsonLdError {
		final Map<String, Object> jsonLd =
				wrappedIntoGraphIfMissing((Map<String, Object>) JsonLdProcessor.compact(
						JSONUtils.fromString(source), contextObject, new JsonLdOptions()));
		jsonLd.put("@id", id + "/about");
		return jsonLd;
	}

	private static Map<String, Object> wrappedIntoGraphIfMissing(
			final Map<String, Object> jsonLd) {
		final String graphKey = "@graph";
		final String contextKey = "@context";
		if (!jsonLd.containsKey(graphKey)) {
			final Map<String, Object> newJsonLd = new HashMap<>();
			final Map<String, Object> graph = new HashMap<>();
			newJsonLd.put(contextKey, jsonLd.get(contextKey));
			jsonLd.remove(contextKey);
			graph.putAll(jsonLd);
			newJsonLd.put(graphKey, Arrays.asList(graph));
			return newJsonLd;
		}
		return jsonLd;
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
		final String json = getSourceWithFullProperties();
		String result = "";
		try {
			result = converter.toRdf(json);
		} catch (BadURIException x) {
			Logger.error(x.getMessage(), x);
		}
		return result;
	}

}
