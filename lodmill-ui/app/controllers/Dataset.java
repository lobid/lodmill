/* Copyright 2014 Pascal Christoph, hbz. Licensed under the Eclipse Public License 1.0 */

package controllers;

import play.mvc.Controller;
import play.mvc.Result;

/**
 * Path controller. Implements path-style routes and `about` redirects.
 * 
 * @author pascal
 */
public final class Dataset extends Controller {

	private Dataset() {
		/* No instantiation */
	}

	/**
	 * Redirect to {@link #resourceAbout(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result resource(final String id, final String format) {
		return redirect(routes.Dataset.resourceAbout(format));
	}

	/***/
	@SuppressWarnings("javadoc")
	public static Result resourceAbout(final String id, final String format) {
		return ok("ID: " + id); // TODO get dataset for ID from ES
	}

}