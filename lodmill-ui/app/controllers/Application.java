/* Copyright 2012-2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package controllers;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;

import models.Document;
import models.Index;
import models.Parameter;
import models.Search;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import play.Logger;
import play.api.http.MediaRange;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Main application controller.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class Application extends Controller {

	private Application() { // NOPMD
		/* No instantiation */
	}

	/**
	 * @return The main page.
	 */
	public static Result index() {
		return ok(views.html.index.render());
	}

	/**
	 * @return The API page.
	 */
	public static Result api() {
		return ok(views.html.api.render());
	}

	/**
	 * @return The main page.
	 */
	public static Result contact() {
		return ok(views.html.contact.render());
	}

	/**
	 * @return The main page.
	 */
	public static Result about() {
		return ok(views.html.about.render());
	}

	/**
	 * Search enpoint for actual queries.
	 * 
	 * @param indexParameter The index to search (see {@link Index}).
	 * @param parameter The search parameter type (see {@link Parameter}).
	 * @param queryParameter The search query
	 * @param formatParameter The result format
	 * @param from The start index of the result set
	 * @param size The size of the result set
	 * @param format The result format requested
	 * @param owner The ID of an owner holding items of the requested resources
	 * @param set The ID of a set the requested resources should be part of
	 * @param type The type of the requestes resources
	 * @return The results, in the format specified
	 */
	static Result search(final Index index, final Parameter parameter,
			final String queryParameter, final String formatParameter,
			final int from, final int size, final String owner, final String set,
			final String type) {
		List<Document> docs = new ArrayList<>();
		Pair<String, String> fieldAndFormat;
		try {
			fieldAndFormat = getFieldAndFormat(formatParameter);
			docs =
					new Search(queryParameter, index, parameter).page(from, size)
							.field(fieldAndFormat.getLeft()).owner(owner).set(set).type(type)
							.documents();
		} catch (IllegalArgumentException e) {
			Logger.error(e.getMessage(), e);
			return badRequest(e.getMessage());
		}
		final ImmutableMap<ResultFormat, Result> results =
				results(queryParameter, docs, index, fieldAndFormat.getLeft());
		try {
			return results.get(ResultFormat.valueOf(fieldAndFormat.getRight()
					.toUpperCase()));
		} catch (IllegalArgumentException e) {
			Logger.error(e.getMessage(), e);
			return badRequest("Invalid 'format' parameter, use one of: "
					+ Joiner.on(", ").join(results.keySet()).toLowerCase());
		}
	}

	private static Pair<String, String> getFieldAndFormat(final String format) {
		if (format.contains(".")) {
			final String[] strings = format.split("\\.");
			if (strings.length != 2 || !strings[0].equals("short"))
				throw new IllegalArgumentException(
						"Parameter modifier only supported on `short` format, "
								+ "e.g. `format=short.fulltextOnline`.");
			return new ImmutablePair<>(strings[1], "full");
		}
		return new ImmutablePair<>("", format);
	}

	private static Function<Document, JsonNode> jsonFull =
			new Function<Document, JsonNode>() {
				@Override
				public JsonNode apply(final Document doc) {
					return Json.parse(doc.getSource());
				}
			};

	private static Function<Document, String> jsonShort =
			new Function<Document, String>() {
				@Override
				public String apply(final Document doc) {
					return doc.getMatchedField();
				}
			};

	private static Function<Document, JsonNode> jsonLabelValue =
			new Function<Document, JsonNode>() {
				@Override
				public JsonNode apply(final Document doc) {
					final ObjectNode object = Json.newObject();
					object.put("label", doc.getMatchedField());
					object.put("value", doc.getId());
					return object;
				}
			};

	private static ImmutableMap<ResultFormat, Result> results(final String query,
			final List<Document> documents, final Index selectedIndex,
			final String field) {
		/* JSONP callback support for remote server calls with JavaScript: */
		final String[] callback =
				request() == null || request().queryString() == null ? null : request()
						.queryString().get("callback");
		final ImmutableMap<ResultFormat, Result> results =
				new ImmutableMap.Builder<ResultFormat, Result>()
						.put(ResultFormat.NEGOTIATE,
								negotiateContent(documents, selectedIndex, query, field))
						.put(ResultFormat.FULL,
								withCallback(callback, fullJsonResponse(documents, field)))
						.put(
								ResultFormat.SHORT,
								withCallback(callback, Json.toJson(new LinkedHashSet<>(Lists
										.transform(documents, jsonShort)))))
						.put(
								ResultFormat.IDS,
								withCallback(callback,
										Json.toJson(Lists.transform(documents, jsonLabelValue))))
						.build();
		return results;
	}

	private static Status withCallback(final String[] callback,
			final JsonNode shortJson) {
		return callback != null ? ok(String
				.format("%s(%s)", callback[0], shortJson)) : ok(shortJson);
	}

	private static final Predicate<JsonNode> nonEmptyNode =
			new Predicate<JsonNode>() {
				@Override
				public boolean apply(JsonNode node) {
					return node.size() > 0;
				}
			};

	private static final Function<JsonNode, List<JsonNode>> nodeToArray =
			new Function<JsonNode, List<JsonNode>>() {
				@Override
				public List<JsonNode> apply(JsonNode input) {
					return input.isArray() ? /**/
					Lists.newArrayList(input.elements()) : Lists.newArrayList(input);
				}
			};

	private static final Comparator<JsonNode> nodeAsTextComparator =
			new Comparator<JsonNode>() {
				@Override
				public int compare(JsonNode o1, JsonNode o2) {
					return o1.asText().compareTo(o2.asText());
				}
			};

	private static JsonNode fullJsonResponse(final List<Document> documents,
			final String field) {
		Iterable<JsonNode> nonEmptyNodes =
				Iterables.filter(Lists.transform(documents, jsonFull), nonEmptyNode);
		if (!field.isEmpty()) {
			nonEmptyNodes =
					ImmutableSortedSet.copyOf(nodeAsTextComparator,
							FluentIterable.from(nonEmptyNodes)
									.transformAndConcat(nodeToArray));
		}
		return Json.toJson(ImmutableSet.copyOf(nonEmptyNodes));
	}

	private static Result negotiateContent(List<Document> documents,
			Index selectedIndex, String query, String field) {
		final Status notAcceptable =
				status(406, "Not acceptable: unsupported content type requested\n");
		if (invalidAcceptHeader())
			return notAcceptable;
		for (MediaRange mediaRange : request().acceptedTypes())
			for (Serialization serialization : Serialization.values())
				for (String mimeType : serialization.getTypes())
					if (mediaRange.accepts(mimeType))
						return serialization(documents, selectedIndex, query,
								serialization, field);
		return notAcceptable;
	}

	private static Result serialization(List<Document> documents,
			Index selectedIndex, String query, Serialization serialization,
			String field) {
		switch (serialization) {
		case JSON_LD:
			return ok(fullJsonResponse(documents, field));
		case RDF_A:
			return ok(views.html.docs.render(documents, selectedIndex, query));
		default:
			return ok(Joiner.on("\n").join(transform(documents, serialization)));
		}
	}

	private static boolean invalidAcceptHeader() {
		if (request() == null)
			return true;
		final String acceptHeader = request().getHeader("Accept");
		return (acceptHeader == null || acceptHeader.trim().isEmpty());
	}

	private static List<String> transform(List<Document> documents,
			final Serialization serialization) {
		List<String> transformed =
				Lists.transform(documents, new Function<Document, String>() {
					@Override
					public String apply(final Document doc) {
						return doc.as(serialization.format);
					}
				});
		return transformed;
	}
}