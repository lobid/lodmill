/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package controllers;

import models.Index;
import models.Parameter;
import play.Logger;
import play.core.j.JavaResultExtractor;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.SimpleResult;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
	 * @param name The resource name
	 * @param author The resource author
	 * @param subject The resource subject
	 * @param set The resource set
	 * @param format The result format
	 * @param from The start index of the result set
	 * @param size The size of the result set
	 * @return Matching resources
	 */
	public static Result resource(final String id,
			final String name, // NOPMD
			final String author, final String subject, final String set,
			final String format, final int from, final int size) {
		Logger
				.debug(String
						.format(
								"GET /resource; id: '%s', name: '%s', author: '%s', subject: '%s', format: '%s'",
								id, name, author, subject, format));
		final Index index = Index.LOBID_RESOURCES;
		Result result = null;
		if (defined(id)) {
			result = Application.search(index, Parameter.ID, id, format, from, size);
		} else if (defined(name)) {
			result =
					Application.search(index, Parameter.NAME, name, format, from, size);
		} else if (defined(author)) {
			result =
					Application.search(index, Parameter.AUTHOR, author, format, from,
							size);
		} else if (defined(subject)) {
			result =
					Application.search(index, Parameter.SUBJECT, subject, format, from,
							size);
		} else if (defined(set)) {
			result =
					Application.search(index, Parameter.SET, set, format, from, size);
		}
		return result;
	}

	/**
	 * @param id The item ID
	 * @param name The item name
	 * @param format The result format
	 * @param from The start index of the result set
	 * @param size The size of the result set
	 * @return Matching items
	 */
	public static Result item(final String id, final String name, // NOPMD
			final String format, final int from, final int size) {
		Logger.debug(String.format("GET /item; id: '%s', name: '%s'", id, name));
		return search(id, name, format, from, size, Index.LOBID_ITEMS);
	}

	/**
	 * @param id The organisation ID
	 * @param name The organisation name
	 * @param format The result format
	 * @param from The start index of the result set
	 * @param size The size of the result set
	 * @return Matching organisations
	 */
	public static Result organisation(final String id, final String name, // NOPMD
			final String format, final int from, final int size) {
		Logger.debug(String.format("GET /organisation; id: '%s', name: '%s'", id,
				name));
		return search(id, name, format, from, size, Index.LOBID_ORGANISATIONS);
	}

	/**
	 * @param id The person ID
	 * @param name The person name
	 * @param format The result format
	 * @param from The start index of the result set
	 * @param size The size of the result set
	 * @return Matching persons
	 */
	public static Result person(final String id, final String name, // NOPMD
			final String format, final int from, final int size) {
		Logger.debug(String.format("GET /person; id: '%s', name: '%s'", id, name));
		return search(id, name, format, from, size, Index.GND);
	}

	private static Result search(final String id, final String name,
			final String format, final int from, final int size, final Index index) {
		Result result = null;
		if (defined(id)) {
			result = Application.search(index, Parameter.ID, id, format, from, size);
		} else if (defined(name)) {
			result =
					Application.search(index, Parameter.NAME, name, format, from, size);
		}
		return result;
	}

	/**
	 * @param id Some ID
	 * @param name Some name
	 * @param format The result format
	 * @param from The start index of the result set
	 * @param size The size of the result set
	 * @return Matching entities, combined in a JSON map
	 */
	public static Result search(final String id, final String name, // NOPMD
			final String format, final int from, final int size) {
		Logger.debug(String.format("GET /search; id: '%s', name: '%s'", id, name));
		if (format.equals("page")) { // NOPMD
			final String message = "Result format 'page' not supported for /entity";
			Logger.error(message);
			return badRequest(message);
		}
		final ObjectNode json = Json.newObject();
		putIfOk(json, "resource",
				resource(id, name, "", "", "", format, from, size));
		putIfOk(json, "organisation", organisation(id, name, format, from, size));
		putIfOk(json, "person", person(id, name, format, from, size));
		Logger.trace("JSON response: " + json);
		return ok(json);
	}

	private static void putIfOk(final ObjectNode json, final String key,
			final Result result) {
		/* TODO: there's got to be a better way, without casting */
		if (((SimpleResult) result).getWrappedSimpleResult().header().status() == OK) {
			json.put(key, json(result));
		}
	}

	private static JsonNode json(final Result resources) {
		return Json.parse(new String(JavaResultExtractor
				.getBody((SimpleResult) resources)));
	}

	private static boolean defined(final String param) {
		return !param.isEmpty();
	}

}