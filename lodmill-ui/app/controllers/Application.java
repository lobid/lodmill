/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package controllers;

import java.util.ArrayList;
import java.util.List;

import models.Document;
import models.DocumentHelper;
import models.Index;

import org.codehaus.jackson.JsonNode;

import play.api.http.MediaRange;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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

	/*
	 * These static variables are used by the autocomplete function which uses the
	 * Jquery-UI autocomplete widget. According to my current understanding, this
	 * widget requires an endpoint that expects a single String parameter, so we
	 * set the other required info here:
	 */
	/** The index to search in (see {@link Index}). */
	public static Index index = Index.LOBID_RESOURCES;

	/** The search category (see {@link Index#fields()}). */
	public static String category = "author";

	/**
	 * @return The main page.
	 */
	public static Result index() {
		return ok(views.html.index.render(index, "", category, ResultFormat.PAGE
				.toString().toLowerCase()));
	}

	/**
	 * Config endpoint for setting search parameters.
	 * 
	 * @param indexParameter The index to search (see {@link Index}).
	 * @param categoryParameter The search category (see {@link Index#fields()}).
	 * @param formatParameter The result format
	 * @return The search page, with the config set
	 */
	public static Result config(final String indexParameter,
			final String categoryParameter, final String formatParameter) {
		Application.index = Index.id(indexParameter);
		Application.category = categoryParameter;
		return ok(views.html.index.render(Index.id(indexParameter), "",
				categoryParameter, formatParameter));
	}

	/**
	 * Search enpoint for actual queries.
	 * 
	 * @param indexParameter The index to search (see {@link Index}).
	 * @param categoryParameter The search category (see {@link Index#fields()}).
	 * @param formatParameter The result format
	 * @param queryParameter The search query
	 * @return The results, in the format specified
	 */
	public static Result search(final String indexParameter,
			final String categoryParameter, final String formatParameter,
			final String queryParameter) {
		List<Document> docs = new ArrayList<>();
		Index selectedIndex = Index.id(indexParameter);
		try {
			docs =
					DocumentHelper.search(queryParameter, selectedIndex,
							categoryParameter);
		} catch (IllegalArgumentException e) {
			return badRequest(e.getMessage());
		}
		final ImmutableMap<ResultFormat, Result> results =
				results(queryParameter, docs, selectedIndex);
		try {
			return results.get(ResultFormat.valueOf(formatParameter.toUpperCase()));
		} catch (IllegalArgumentException e) {
			return badRequest("Invalid 'format' parameter, use one of: "
					+ Joiner.on(", ").join(results.keySet()).toLowerCase());
		}
	}

	/**
	 * @param term The term to auto-complete
	 * @return A list of completion suggestions for the given term
	 */
	public static Result autocomplete(final String term) {
		return results(term, DocumentHelper.search(term, index, category), index)
				.get(ResultFormat.SHORT);
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

	private static ImmutableMap<ResultFormat, Result> results(final String query,
			final List<Document> documents, final Index selectedIndex) {
		/* JSONP callback support for remote server calls with JavaScript: */
		final String[] callback =
				request() == null || request().queryString() == null ? null : request()
						.queryString().get("callback");
		final JsonNode shortJson =
				Json.toJson(ImmutableSet.copyOf(Lists.transform(documents, jsonShort)));
		final ImmutableMap<ResultFormat, Result> results =
				new ImmutableMap.Builder<ResultFormat, Result>()
						.put(ResultFormat.PAGE,
								ok(views.html.docs.render(documents, selectedIndex, query)))
						.put(ResultFormat.FULL,
								negotiateContent(documents, selectedIndex, query))
						.put(
								ResultFormat.SHORT,
								callback != null ? ok(String.format("%s(%s)", callback[0],
										shortJson)) : ok(shortJson)).build();
		return results;
	}

	private static Result negotiateContent(List<Document> documents,
			Index selectedIndex, String query) {
		if (accepted(Serialization.JSON_LD)) {
			return ok(Json.toJson(ImmutableSet.copyOf(Lists.transform(documents,
					jsonFull))));
		} else if (accepted(Serialization.RDF_A)) {
			return ok(views.html.docs.render(documents, selectedIndex, query));
		}
		for (final Serialization serialization : Serialization.values()) {
			if (accepted(serialization)) {
				return ok(Joiner.on("\n").join(transform(documents, serialization)));
			}
		}
		return status(406, "Not acceptable: unsupported content type requested\n");
	}

	private static boolean accepted(Serialization serialization) {
		return request() != null
		/* Any of the types associated with the serialization... */
		&& Iterables.any(serialization.types, new Predicate<String>() {
			@Override
			public boolean apply(final String mediaType) {
				/* ...is accepted by any of the accepted types of the request: */
				return Iterables.any(request().acceptedTypes(),
						new Predicate<MediaRange>() {
							@Override
							public boolean apply(MediaRange media) {
								return media.accepts(mediaType);
							}
						});
			}
		});
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