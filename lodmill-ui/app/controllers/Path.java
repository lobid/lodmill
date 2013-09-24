/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package controllers;

import play.mvc.Controller;
import play.mvc.Result;

/**
 * Path controller. Implements path-style routes and `about` redirects.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class Path extends Controller {

	private Path() {
		/* No instantiation */
	}

	/**
	 * Redirect to
	 * {@link #resourceAbout(String, String, String, String, String, String, int, int)}
	 */
	@SuppressWarnings("javadoc")
	public static Result resource(final String id, final String name,
			final String author, final String subject, final String set,
			final String format, final int from, final int size) {
		return redirect(routes.Path.resourceAbout(id, name, author, subject, set,
				format, from, size));
	}

	/**
	 * Return
	 * {@link Api#resource(String, String, String, String, String, String, int, int)}
	 */
	@SuppressWarnings("javadoc")
	public static Result resourceAbout(final String id, final String name,
			final String author, final String subject, final String set,
			final String format, final int from, final int size) {
		return Api.resource(id, name, author, subject, set, format, from, size);
	}

	/** Redirect to {@link #itemAbout(String, String, String, int, int)} */
	@SuppressWarnings("javadoc")
	public static Result item(final String id, final String name,
			final String format, final int from, final int size) {
		return redirect(routes.Path.itemAbout(id, name, format, from, size));
	}

	/** Return {@link Api#item(String, String, String, int, int)} */
	@SuppressWarnings("javadoc")
	public static Result itemAbout(final String id, final String name,
			final String format, final int from, final int size) {
		return Api.item(id, name, format, from, size);
	}

	/** Redirect to {@link #organisationAbout(String, String, String, int, int)} */
	@SuppressWarnings("javadoc")
	public static Result organisation(final String id, final String name,
			final String format, final int from, final int size) {
		return redirect(routes.Path.organisationAbout(id, name, format, from, size));
	}

	/** Return {@link Api#organisation(String, String, String, int, int)} */
	@SuppressWarnings("javadoc")
	public static Result organisationAbout(final String id, final String name,
			final String format, final int from, final int size) {
		return Api.organisation(id, name, format, from, size);
	}

	/** Redirect to {@link #personAbout(String, String, String, int, int)} */
	@SuppressWarnings("javadoc")
	public static Result person(final String id, final String name,
			final String format, final int from, final int size) {
		return redirect(routes.Path.personAbout(id, name, format, from, size));
	}

	/** Return {@link Api#person(String, String, String, int, int)} */
	@SuppressWarnings("javadoc")
	public static Result personAbout(final String id, final String name,
			final String format, final int from, final int size) {
		return Api.person(id, name, format, from, size);
	}
}