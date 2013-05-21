/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package controllers;

import models.Index;
import play.Logger;
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
			result = Application.search(index.id(), "id", format, id);
		} else if (defined(name)) {
			result = // TODO: implement resource-by-name
					badRequest("Parameter 'name' currently not supported for GET /resource");
		} else if (defined(author)) {
			result = Application.search(index.id(), "author", format, author);
		} else if (defined(subject)) {
			result = Application.search(index.id(), "keyword", format, subject);
			// TODO: use 'subject' internally (not 'keyword')
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
		if (defined(id)) {
			result = // TODO: implement organisation-by-id
					badRequest("Parameter 'id' currently not supported for GET /organisation");
		} else if (defined(name)) {
			result = Application.search(index.id(), "title", format, name);
			// TODO: use 'name' internally (not 'title')
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
		if (defined(id)) {
			result = // TODO: implement person-by-id
					badRequest("Parameter 'id' currently not supported for GET /person");
		} else if (defined(name)) {
			result = Application.search(index.id(), "author", format, name);
			// TODO: use 'name' internally (not 'author')
		}
		return result;
	}

	private static boolean defined(final String param) {
		return !param.isEmpty();
	}

}