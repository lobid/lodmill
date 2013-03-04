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

	public enum Format {
		PAGE, FULL, SHORT
	}

	/*
	 * These static variables are used by the autocomplete function which uses
	 * the Jquery-UI autocomplete widget. According to my current understanding,
	 * this widget requires an endpoint that expects a single String parameter,
	 * so we set the other required info here:
	 */
	public static String index = "lobid-index";
	public static String category = "author";

	public static Result index() {
		return results("", new ArrayList<Document>(), index, category).get(
				Format.PAGE);
	}

	public static Result config(String index, String category, String format) {
		Application.index = index;
		Application.category = category;
		return ok(views.html.index.render(new ArrayList<Document>(), index, "",
				category, format));
	}

	public static Result search(final String index, final String category,
			final String format, final String query) {
		List<Document> docs = new ArrayList<>();
		try {
			docs = Document.search(query, index, category);
		} catch (IllegalArgumentException e) {
			return badRequest(e.getMessage());
		}
		final ImmutableMap<Format, Result> results =
				results(query, docs, index, category);
		try {
			return results.get(Format.valueOf(format.toUpperCase()));
		} catch (IllegalArgumentException e) {
			return badRequest("Invalid 'format' parameter, use one of: "
					+ Joiner.on(", ").join(results.keySet()).toLowerCase());
		}
	}

	public static Result autocomplete(final String term) {
		return results(term, Document.search(term, index, category), index,
				category).get(Format.SHORT);
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
			final List<Document> documents, final String index,
			final String category) {
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
								ok(views.html.index.render(documents, index,
										query, category, Format.PAGE.toString()
												.toLowerCase())))
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