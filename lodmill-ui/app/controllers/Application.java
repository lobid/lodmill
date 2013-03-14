/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package controllers;

import java.util.ArrayList;
import java.util.List;

import models.Document;

import org.codehaus.jackson.JsonNode;

import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
	 * The result format.
	 */
	public enum Format {
		/** Complete HTML page with search form on top, results at bottom. */
		PAGE,
		/** The fulle JSON representation from the index. */
		FULL,
		/** Short results strings for auto-completion suggestions. */
		SHORT
	}

	/*
	 * These static variables are used by the autocomplete function which uses the
	 * Jquery-UI autocomplete widget. According to my current understanding, this
	 * widget requires an endpoint that expects a single String parameter, so we
	 * set the other required info here:
	 */
	/** The index to search in (see {@link Document#searchFieldsMap}). */
	public static String index = "lobid-index";

	/** The search category (see {@link Document#searchFieldsMap}). */
	public static String category = "author";

	/**
	 * @return The main page.
	 */
	public static Result index() {
		return results("", new ArrayList<Document>(), index, category).get(
				Format.PAGE);
	}

	/**
	 * Config endpoint for setting search parameters.
	 * 
	 * @param indexParameter The index to search (see
	 *          {@link Document#searchFieldsMap}).
	 * @param categoryParameter The search category (see
	 *          {@link Document#searchFieldsMap}).
	 * @param formatParameter The result format
	 * @return The search page, with the config set
	 */
	public static Result config(final String indexParameter,
			final String categoryParameter, final String formatParameter) {
		Application.index = indexParameter;
		Application.category = categoryParameter;
		return ok(views.html.index.render(new ArrayList<Document>(),
				indexParameter, "", categoryParameter, formatParameter));
	}

	/**
	 * Search enpoint for actual queries.
	 * 
	 * @param indexParameter The index to search (see
	 *          {@link Document#searchFieldsMap}).
	 * @param categoryParameter The search category (see
	 *          {@link Document#searchFieldsMap}).
	 * @param formatParameter The result format
	 * @param queryParameter The search query
	 * @return The results, in the format specified
	 */
	public static Result search(final String indexParameter,
			final String categoryParameter, final String formatParameter,
			final String queryParameter) {
		List<Document> docs = new ArrayList<>();
		try {
			docs = Document.search(queryParameter, indexParameter, categoryParameter);
		} catch (IllegalArgumentException e) {
			return badRequest(e.getMessage());
		}
		final ImmutableMap<Format, Result> results =
				results(queryParameter, docs, indexParameter, categoryParameter);
		try {
			return results.get(Format.valueOf(formatParameter.toUpperCase()));
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
		return results(term, Document.search(term, index, category), index,
				category).get(Format.SHORT);
	}

	private static Function<Document, JsonNode> jsonFull =
			new Function<Document, JsonNode>() {
				@Override
				public JsonNode apply(final Document doc) {
					return Json.parse(doc.source);
				}
			};

	private static Function<Document, String> jsonShort =
			new Function<Document, String>() {
				@Override
				public String apply(final Document doc) {
					return doc.matchedField;
				}
			};

	private static ImmutableMap<Format, Result> results(final String query,
			final List<Document> documents, final String selectedIndex,
			final String selectedCategory) {
		/* JSONP callback support for remote server calls with JavaScript: */
		final String[] callback =
				request() == null || request().queryString() == null ? null : request()
						.queryString().get("callback");
		final JsonNode shortJson =
				Json.toJson(ImmutableSet.copyOf(Lists.transform(documents, jsonShort)));
		final ImmutableMap<Format, Result> results =
				new ImmutableMap.Builder<Format, Result>()
						.put(
								Format.PAGE,
								ok(views.html.index.render(documents, selectedIndex, query,
										selectedCategory, Format.PAGE.toString().toLowerCase())))
						.put(
								Format.FULL,
								ok(Json.toJson(ImmutableSet.copyOf(Lists.transform(documents,
										jsonFull)))))
						.put(
								Format.SHORT,
								callback != null ? ok(String.format("%s(%s)", callback[0],
										shortJson)) : ok(shortJson)).build();
		return results;
	}

}