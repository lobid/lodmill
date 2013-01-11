/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package controllers;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

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

	private enum Format {
		PAGE, FULL, SHORT
	}

	public static final List<String> INDEXES = new ArrayList<>(new TreeSet<>(
			Document.searchFieldsMap.keySet()));
	public static String index = "lobid-index"; // NOPMD
	private static List<Document> docs = new ArrayList<>();

	public static Result index() {
		return redirect(routes.Application.search());
	}

	public static Result search() {
		if (request().queryString().isEmpty()) {
			return results("", new ArrayList<Document>()).get(Format.PAGE);
		}
		final String query = request().queryString().get("query")[0];
		index = request().queryString().get("index")[0];
		docs = Document.search(query, index);
		final ImmutableMap<Format, Result> results = results(query, docs);
		final String format = request().queryString().get("format")[0];
		try {
			return results.get(Format.valueOf(format.toUpperCase()));
		} catch (IllegalArgumentException e) {
			return badRequest("Invalid 'format' parameter, use one of: "
					+ Joiner.on(", ").join(results.keySet()).toLowerCase());
		}
	}

	public static Result autocomplete(final String term) {
		return results(term, Document.search(term, index)).get(Format.SHORT);
	}

	private static Function<Document, JsonNode> jsonFull =
			new Function<Document, JsonNode>() {
				public JsonNode apply(final Document doc) {
					return Json.parse(doc.source);
				}
			};

	private static Function<Document, String> jsonShort =
			new Function<Document, String>() {
				public String apply(final Document doc) {
					return doc.matchedField;
				}
			};

	private static ImmutableMap<Format, Result> results(final String query,
			final List<Document> documents) {
		/* JSONP callback support for remote server calls with JavaScript: */
		final String[] callback =
				request() == null || request().queryString() == null ? null
						: request().queryString().get("callback");
		final JsonNode shortJson =
				Json.toJson(ImmutableSet.copyOf(Lists.transform(documents,
						jsonShort)));
		final ImmutableMap<Format, Result> results =
				new ImmutableMap.Builder<Format, Result>()
						.put(Format.PAGE,
								ok(views.html.index.render(documents,
										INDEXES.indexOf(index), query)))
						.put(Format.FULL,
								ok(Json.toJson(ImmutableSet.copyOf(Lists
										.transform(documents, jsonFull)))))
						.put(Format.SHORT,
								callback != null ? ok(String.format("%s(%s)",
										callback[0], shortJson))
										: ok(shortJson)).build();
		return results;
	}

}