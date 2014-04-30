/* Copyright 2014 Pascal Christoph, hbz. Licensed under the Eclipse Public License 1.0 */

package controllers;

import play.mvc.Controller;
import play.mvc.Result;

/**
 * Dataset controller. Implements path-style routes and `about` redirects.
 * 
 * @author Pascal Christoph (dr0i)
 */
public final class LobidPerson extends Controller {
	// TODO don't hardwire index name to switch easily between staging and
	// production
	static final String DATA_INDEX = "lobid-persons";
	static final String INDEX_TYPE = "json-ld-lobid-person";

	private LobidPerson() {
		/* No instantiation */
	}

	/**
	 * Redirects to {@link #resourceAboutRPB(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result ap(final String format) {
		return redirect(routes.LobidPerson.apAbout(format));
	}

	/**
	 * Returns {@link #resourceAbout(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result apAbout(final String id, final String format) {
		return Dataset.getId(id, format, DATA_INDEX, INDEX_TYPE);
	}

	/**
	 * Redirects to {@link #resourceAboutRPB(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result fs(final String format) {
		return redirect(routes.LobidPerson.fsAbout(format));
	}

	/**
	 * Returns {@link #resourceAbout(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result fsAbout(final String id, final String format) {
		return Dataset.getId(id, format, DATA_INDEX, INDEX_TYPE);
	}

	/**
	 * Redirects to {@link #resourceAboutRPB(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result pc(final String format) {
		return redirect(routes.LobidPerson.pcAbout(format));
	}

	/**
	 * Returns {@link #resourceAbout(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result pcAbout(final String id, final String format) {
		return Dataset.getId(id, format, DATA_INDEX, INDEX_TYPE);
	}
}