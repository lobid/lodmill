/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package controllers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import models.Document;

import org.codehaus.jackson.JsonNode;
import org.lobid.lodmill.JsonLdConverter;

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

	/**
	 * The different indices to use.
	 */
	public enum Index {
		/***/
		LOBID_RESOURCES("lobid-index"), /***/
		LOBID_ORGANISATIONS("lobid-orgs-index"), /***/
		GND("gnd-index");
		private String id;

		Index(String name) {
			this.id = name;
		}

		/** @return The Elasticsearch index name. */
		public String id() {
			return id;
		}

		/**
		 * @param id The Elasticsearch index name
		 * @return The index enum element with the given id
		 * @throws IllegalArgumentException if there is no index with the id
		 */
		public static Index id(String id) {
			for (Index i : values()) {
				if (i.id().equals(id)) {
					return i;
				}
			}
			throw new IllegalArgumentException("No such index: " + id);
		}
	}

	/*
	 * These static variables are used by the autocomplete function which uses the
	 * Jquery-UI autocomplete widget. According to my current understanding, this
	 * widget requires an endpoint that expects a single String parameter, so we
	 * set the other required info here:
	 */
	/** The index to search in (see {@link Document#searchFieldsMap}). */
	public static Index index = Index.LOBID_RESOURCES;

	/** The search category (see {@link Document#searchFieldsMap}). */
	public static String category = "author";

	/**
	 * @return The main page.
	 */
	public static Result index() {
		return ok(views.html.index.render(index, "", category, Format.PAGE
				.toString().toLowerCase()));
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
		Application.index = Index.id(indexParameter);
		Application.category = categoryParameter;
		return ok(views.html.index.render(Index.id(indexParameter), "",
				categoryParameter, formatParameter));
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
		Index selectedIndex = Index.id(indexParameter);
		try {
			docs = Document.search(queryParameter, selectedIndex, categoryParameter);
		} catch (IllegalArgumentException e) {
			return badRequest(e.getMessage());
		}
		final ImmutableMap<Format, Result> results =
				results(queryParameter, docs, selectedIndex);
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
		return results(term, Document.search(term, index, category), index).get(
				Format.SHORT);
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

	private static ImmutableMap<Format, Result> results(final String query,
			final List<Document> documents, final Index selectedIndex) {
		/* JSONP callback support for remote server calls with JavaScript: */
		final String[] callback =
				request() == null || request().queryString() == null ? null : request()
						.queryString().get("callback");
		final JsonNode shortJson =
				Json.toJson(ImmutableSet.copyOf(Lists.transform(documents, jsonShort)));
		final ImmutableMap<Format, Result> results =
				new ImmutableMap.Builder<Format, Result>()
						.put(Format.PAGE,
								ok(views.html.docs.render(documents, selectedIndex, query)))
						.put(Format.FULL, negotiateContent(documents, selectedIndex, query))
						.put(
								Format.SHORT,
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

	/** Supported RDF serializations for content negotiation. */
	@SuppressWarnings("javadoc")
	/* no javadoc for elements */
	public enum Serialization {/* @formatter:off */
		JSON_LD(null, Arrays.asList("application/json", "application/ld+json")),
		RDF_A(null, Arrays.asList("text/html", "text/xml", "application/xml")),
		N_TRIPLE(JsonLdConverter.Format.N_TRIPLE, Arrays.asList("text/plain")),
		N3(JsonLdConverter.Format.N3, Arrays.asList("text/rdf+n3", "text/n3")),
		TURTLE(JsonLdConverter.Format.TURTLE, /* @formatter:on */
				Arrays.asList("application/x-turtle", "text/turtle"));

		private JsonLdConverter.Format format;
		private List<String> types;

		/** @return The content types associated with this serialization. */
		public List<String> getTypes() {
			return types;
		}

		private Serialization(JsonLdConverter.Format format, List<String> types) {
			this.format = format;
			this.types = types;
		}
	}

	/** Different ways of serializing a table row (used fo RDFa output) */
	@SuppressWarnings("javadoc")
	/* no javadoc for elements */
	public enum TableRow {
		SINGLE_VALUE, SINGLE_LINK, MULTI_VALUE, MULTI_LINK, SINGLE_IMAGE
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