/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package controllers;

import models.Index;
import models.Parameter;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

import play.Logger;
import play.api.mvc.PlainResult;
import play.core.j.JavaResultExtractor;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

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
	 * @param format The result format
	 * @return Matching resources
	 */
	public static Result resource(final String id, final String name, // NOPMD
			final String author, final String subject, final String format) {
		Logger.debug(String.format(
				"GET /resource; id: '%s', name: '%s', author: '%s', subject: '%s'", id,
				name, author, subject));
		final Index index = Index.LOBID_RESOURCES;
		Result result = null;
		if (defined(id)) {
			result = Application.search(index, Parameter.ID, id, format);
		} else if (defined(name)) { // TODO: implement resource-by-name
			result = Application.search(index, Parameter.NAME, name, format);
		} else if (defined(author)) {
			result = Application.search(index, Parameter.AUTHOR, author, format);
		} else if (defined(subject)) {
			result = Application.search(index, Parameter.SUBJECT, subject, format);
		}
		return result;
	}

	/**
	 * @param id The organisation ID
	 * @param name The organisation name
	 * @param format The result format
	 * @return Matching organisations
	 */
	public static Result organisation(final String id, final String name, // NOPMD
			final String format) {
		Logger.debug(String.format("GET /organisation; id: '%s', name: '%s'", id,
				name));
		final Index index = Index.LOBID_ORGANISATIONS;
		Result result = null;
		if (defined(id)) { // TODO: implement organisation-by-id
			result = Application.search(index, Parameter.ID, id, format);
		} else if (defined(name)) {
			result = Application.search(index, Parameter.NAME, name, format);
		}
		return result;
	}

	/**
	 * @param id The person ID
	 * @param name The person name
	 * @param format The result format
	 * @return Matching persons
	 */
	public static Result person(final String id, final String name, // NOPMD
			final String format) {
		Logger.debug(String.format("GET /person; id: '%s', name: '%s'", id, name));
		final Index index = Index.GND;
		Result result = null;
		if (defined(id)) { // TODO: implement person-by-id
			result = Application.search(index, Parameter.ID, id, format);
		} else if (defined(name)) {
			result = Application.search(index, Parameter.NAME, name, format);
		}
		return result;
	}

	/**
	 * @param id Some ID
	 * @param name Some name
	 * @param format The result format
	 * @return Matching entities, combined in a JSON map
	 */
	public static Result search(final String id, final String name, // NOPMD
			final String format) {
		Logger.debug(String.format("GET /search; id: '%s', name: '%s'", id, name));
		if (format.equals("page")) { // NOPMD
			final String message = "Result format 'page' not supported for /entity";
			Logger.error(message);
			return badRequest(message);
		}
		final ObjectNode json = Json.newObject();
		putIfOk(json, "resource", resource(id, name, "", "", format));
		putIfOk(json, "organisation", organisation(id, name, format));
		putIfOk(json, "person", person(id, name, format));
		Logger.trace("JSON response: " + json);
		return ok(json);
	}

	private static void putIfOk(final ObjectNode json, final String key,
			final Result result) {
		/* TODO: there's got to be a better way, without casting */
		if (((PlainResult) result.getWrappedResult()).header().status() == OK) {
			json.put(key, json(result));
		}
	}

	private static JsonNode json(final Result resources) {
		return Json.parse(new String(JavaResultExtractor.getBody(resources)));
	}

	private static boolean defined(final String param) {
		return !param.isEmpty();
	}

}