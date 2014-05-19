/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package controllers;

import java.util.List;
import java.util.Map;

import models.Index;
import models.Parameter;
import play.Logger;
import play.libs.F.Function;
import play.libs.F.Promise;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import play.test.Helpers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

/**
 * API controller.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class Api extends Controller {

	private Api() { // NOPMD
		/* No instantiation */
	}

	/**
	 * @param id The resource ID
	 * @param q A general query string
	 * @param name The resource name
	 * @param author The resource author
	 * @param subject The resource subject
	 * @param set The resource set
	 * @param format The result format
	 * @param from The start index of the result set
	 * @param size The size of the result set
	 * @param owner The ID of an owner holding items of the requested resources
	 * @param type The type of the requested resources
	 * @param addQueryInfo If true, add a query info object to the response
	 * @return Matching resources
	 */
	public static Promise<Result> resource(
			final String id,
			final String q,
			final String name, // NOPMD
			final String author, final String subject, final String set,
			final String format, final int from, final int size, final String owner,
			final String type, final boolean addQueryInfo) {
		Logger
				.debug(String
						.format(
								"GET /resource; id: '%s', q: '%s', name: '%s', author: '%s', subject: '%s', set: '%s', format: '%s', owner: '%s'",
								id, q, name, author, subject, set, format, owner));
		final Index index = Index.LOBID_RESOURCES;
		final Map.Entry<Parameter, String> parameter =
				Parameter.select(new ImmutableMap.Builder<Parameter, String>() /*@formatter:off*/
						.put(Parameter.ID, id)
						.put(Parameter.Q, q)
						.put(Parameter.NAME, name)
						.put(Parameter.AUTHOR, author)
						.put(Parameter.SUBJECT, subject)
						.put(Parameter.SET, set).build());/*@formatter:on*/
		return Application.search(index, parameter.getKey(), parameter.getValue(),
				format, from, size, owner, set, type, addQueryInfo);
	}

	/**
	 * @param id The item ID
	 * @param q A general query string
	 * @param name The item name
	 * @param format The result format
	 * @param from The start index of the result set
	 * @param size The size of the result set
	 * @param type The type of the requested items
	 * @param addQueryInfo If true, add a query info object to the response
	 * @return Matching items
	 */
	public static Promise<Result> item(final String id,
			final String q,
			final String name, // NOPMD
			final String format, final int from, final int size, final String type,
			final boolean addQueryInfo) {
		Logger.debug(String.format("GET /item; id: '%s', q: '%s', name: '%s'", id,
				q, name));
		return search(id, q, name, format, from, size, Index.LOBID_ITEMS, type,
				addQueryInfo);
	}

	/**
	 * @param id The organisation ID
	 * @param q A general query string
	 * @param name The organisation name
	 * @param format The result format
	 * @param from The start index of the result set
	 * @param size The size of the result set
	 * @param type The type of the requested organisations
	 * @param addQueryInfo If true, add a query info object to the response
	 * @return Matching organisations
	 */
	public static Promise<Result> organisation(final String id,
			final String q,
			final String name, // NOPMD
			final String format, final int from, final int size, final String type,
			final boolean addQueryInfo) {
		Logger.debug(String.format(
				"GET /organisation; id: '%s', name: '%s', q: '%s'", id, name, q));
		return search(id, q, name, format, from, size, Index.LOBID_ORGANISATIONS,
				type, addQueryInfo);
	}

	/**
	 * @param id The person ID
	 * @param q A general query string
	 * @param name The person name
	 * @param format The result format
	 * @param from The start index of the result set
	 * @param size The size of the result set
	 * @param type The type of the requested persons
	 * @param addQueryInfo If true, add a query info object to the response
	 * @return Matching persons
	 */
	public static Promise<Result> person(final String id,
			final String q,
			final String name, // NOPMD
			final String format, final int from, final int size, final String type,
			final boolean addQueryInfo) {
		Logger.debug(String.format("GET /person; id: '%s', q: '%s', name: '%s'",
				id, q, name));
		return search(id, q, name, format, from, size, Index.GND, type,
				addQueryInfo);
	}

	/**
	 * @param id The subject ID
	 * @param q A general query string
	 * @param name The subject name
	 * @param format The result format
	 * @param from The start index of the result set
	 * @param size The size of the result set
	 * @param type The type of the requested subjects
	 * @return Matching subjects
	 */
	public static Promise<Result> subject(final String id, final String q,
			final String name, // NOPMD
			final String format, final int from, final int size, final String type) {
		Logger.debug(String.format("GET /subject; id: '%s', q: '%s', name: '%s'",
				id, q, name));
		final Map.Entry<Parameter, String> parameter =/*@formatter:off*/
				Parameter.select(new ImmutableMap.Builder<Parameter, String>()
						.put(Parameter.ID, id)
						.put(Parameter.Q, q)
						.put(Parameter.SUBJECT, name).build());/*@formatter:on*/
		return Application.search(Index.GND, parameter.getKey(),
				parameter.getValue(), format, from, size, "", "", type, true);
	}

	private static Promise<Result> search(final String id, final String q,
			final String name, final String format, final int from, final int size,
			final Index index, final String type, final boolean addQueryInfo) {
		final Map.Entry<Parameter, String> parameter =/*@formatter:off*/
				Parameter.select(new ImmutableMap.Builder<Parameter, String>()
						.put(Parameter.ID, id)
						.put(Parameter.Q, q)
						.put(Parameter.NAME, name).build());/*@formatter:on*/
		return Application.search(index, parameter.getKey(), parameter.getValue(),
				format, from, size, "", "", type, addQueryInfo);
	}

	/**
	 * @param id Some ID
	 * @param q A general query string
	 * @param name Some name
	 * @param format The result format
	 * @param from The start index of the result set
	 * @param size The size of the result set
	 * @return Matching entities, combined in a JSON map
	 */
	@SuppressWarnings("unchecked")
	public static Promise<Result> search(final String id, final String q,
			final String name, // NOPMD
			final String format, final int from, final int size) {
		Logger.debug(String.format("GET /search; id: '%s', q: '%s', name: '%s'",
				id, q, name));
		if (format.equals("page")) { // NOPMD
			final String message = "Result format 'page' not supported for /entity";
			Logger.error(message);
			return Application.badRequestPromise(message);
		}
		Promise<List<Result>> results =
				Promise
						.sequence(
								resource(id, q, name, "", "", "", format, from, size, "", "",
										true),
								organisation(id, q, name, format, from, size, "", true),
								person(id, q, name, format, from, size, "", true),
								subject(id, q, name, format, from, size, ""));
		return results.map(okJson());
	}

	private static Function<List<Result>, Result> okJson() {
		return new Function<List<Result>, Result>() {
			@Override
			public Result apply(List<Result> results) {
				final ObjectNode json = Json.newObject();
				putIfOk(json, "resource", results.get(0));
				putIfOk(json, "organisation", results.get(1));
				putIfOk(json, "person", results.get(2));
				putIfOk(json, "subject", results.get(3));
				Logger.trace("JSON response: " + json);
				return ok(json);
			}
		};
	}

	private static void putIfOk(final ObjectNode json, final String key,
			final Result result) {
		if (Helpers.status(result) == OK)
			json.put(key, json(result));
	}

	private static JsonNode json(final Result result) {
		return Json.parse(Helpers.contentAsString(result));
	}
}