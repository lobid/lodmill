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
import org.lobid.lodmill.JsonLdConverter;
import org.lobid.lodmill.JsonLdConverter.Format;

import play.Logger;
import play.api.http.MediaRange;
import play.api.templates.Html;
import play.libs.F;
import play.libs.F.Promise;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import scala.concurrent.ExecutionContext;

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
		@SuppressWarnings("unused")
		ExecutionContext ec; // to retain import
	}

	/**
	 * @return The main page.
	 */
	public static Promise<Result> index() {
		return okPromise(views.html.index.render());
	}

	/**
	 * @return The API page.
	 */
	public static Promise<Result> api() {
		return okPromise(views.html.api.render());
	}

	/**
	 * @return The main page.
	 */
	public static Promise<Result> contact() {
		return okPromise(views.html.contact.render());
	}

	/**
	 * @return The main page.
	 */
	public static Promise<Result> about() {
		return okPromise(views.html.about.render());
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
	static Promise<Result> search(final Index index, final Parameter parameter,
			final String queryParameter, final String formatParameter,
			final int from, final int size, final String owner, final String set,
			final String type, final boolean addQueryInfo) {
		Search search;
		try {
			search =
					new Search(queryParameter, index, parameter).page(from, size)
							.field(getFieldAndFormat(formatParameter).getLeft()).owner(owner)
							.set(set).type(type);
		} catch (IllegalArgumentException e) {
			Logger.error(e.getMessage(), e);
			return badRequestPromise(e.getMessage());
		}
		try {
			List<Document> docs = search.documents();
			long allHits = search.totalHits();
			final Promise<ImmutableMap<ResultFormat, Result>> resultPromise =
					resultsPromise(queryParameter, docs, index,
							getFieldAndFormat(formatParameter).getLeft(), allHits,
							addQueryInfo);
			return resultPromise
					.map(new F.Function<ImmutableMap<ResultFormat, Result>, Result>() {
						@Override
						public Result apply(ImmutableMap<ResultFormat, Result> results) {
							return results.get(ResultFormat.valueOf(getFieldAndFormat(
									formatParameter).getRight().toUpperCase()));
						}
					});
		} catch (IllegalArgumentException e) {
			Logger.error(e.getMessage(), e);
			return badRequestPromise("Invalid 'format' parameter, use one of: "
					+ Joiner.on(", ").join(ResultFormat.values()).toLowerCase());
		}
	}

	private static Promise<ImmutableMap<ResultFormat, Result>> resultsPromise(
			final String queryParameter, final List<Document> docs,
			final Index index, final String field, final long allHits,
			final boolean addQueryInfo) {
		return Promise
				.promise(new F.Function0<ImmutableMap<ResultFormat, Result>>() {
					@Override
					public ImmutableMap<ResultFormat, Result> apply() {
						return results(queryParameter, docs, index, field, allHits,
								addQueryInfo);
					}
				});
	}

	static Promise<Result> badRequestPromise(final String message) {
		return Promise.promise(new F.Function0<Result>() {
			@Override
			public Result apply() {
				return badRequest(message);
			}
		});
	}

	static Promise<Result> okPromise(final Html html) {
		return Promise.promise(new F.Function0<Result>() {
			@Override
			public Result apply() {
				return ok(html);
			}
		});
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
			final String field, long allHits, boolean addQueryInfo) {
		/* JSONP callback support for remote server calls with JavaScript: */
		final String[] callback =
				request() == null || request().queryString() == null ? null : request()
						.queryString().get("callback");
		final ImmutableMap<ResultFormat, Result> results =
				new ImmutableMap.Builder<ResultFormat, Result>()
						.put(
								ResultFormat.NEGOTIATE,
								negotiateContent(documents, selectedIndex, query, field,
										allHits, addQueryInfo))
						.put(
								ResultFormat.FULL,
								withCallback(callback,
										fullJsonResponse(documents, field, allHits, addQueryInfo)))
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
			final String field, long allHits, boolean addQueryInfo) {
		Iterable<JsonNode> nonEmptyNodes =
				Iterables.filter(Lists.transform(documents, jsonFull), nonEmptyNode);
		if (!field.isEmpty()) {
			nonEmptyNodes =
					ImmutableSortedSet.copyOf(nodeAsTextComparator,
							FluentIterable.from(nonEmptyNodes)
									.transformAndConcat(nodeToArray));
		}
		List<JsonNode> data = new ArrayList<>();
		if (addQueryInfo)
			data.add(queryInfo(allHits));
		data.addAll(ImmutableSet.copyOf(nonEmptyNodes));
		return Json.toJson(data);
	}

	private static JsonNode queryInfo(long allHits) {
		return Json.toJson(ImmutableMap.of(//
				"@id", "http://lobid.org" + request().uri(),//
				"http://sindice.com/vocab/search#totalResults", allHits));
	}

	static Result negotiateContent(List<Document> documents, Index selectedIndex,
			String query, String field, long allHits, boolean addQueryInfo) {
		final Status notAcceptable =
				status(406, "Not acceptable: unsupported content type requested\n");
		if (invalidAcceptHeader())
			return notAcceptable;
		for (MediaRange mediaRange : request().acceptedTypes())
			for (Serialization serialization : Serialization.values())
				for (String mimeType : serialization.getTypes())
					if (mediaRange.accepts(mimeType))
						return serialization(documents, selectedIndex, query,
								serialization, field, allHits, addQueryInfo);
		return notAcceptable;
	}

	private static Result serialization(List<Document> documents,
			Index selectedIndex, String query, Serialization serialization,
			String field, long allHits, boolean addQueryInfo) {
		switch (serialization) {
		case JSON_LD:
			return ok(fullJsonResponse(documents, field, allHits, addQueryInfo));
		case RDF_A:
			return ok(views.html.docs.render(documents, selectedIndex, query));
		default:
			return ok(Joiner.on("\n").join(
					transform(documents, serialization, allHits, addQueryInfo)));
		}
	}

	private static boolean invalidAcceptHeader() {
		if (request() == null)
			return true;
		final String acceptHeader = request().getHeader("Accept");
		return (acceptHeader == null || acceptHeader.trim().isEmpty());
	}

	private static List<String> transform(List<Document> documents,
			final Serialization serialization, long allHits, boolean addQueryInfo) {
		List<String> transformed = new ArrayList<>();
		if (addQueryInfo)
			transformed.add(transformed(queryInfo(allHits).toString(),
					serialization.format));
		transformed.addAll(Lists.transform(documents,
				new Function<Document, String>() {
					@Override
					public String apply(final Document doc) {
						return doc.as(serialization.format);
					}
				}));
		return transformed;
	}

	private static String transformed(String jsonLdInfo, Format format) {
		final JsonLdConverter converter = new JsonLdConverter(format);
		return converter.toRdf(jsonLdInfo).trim();
	}
}
