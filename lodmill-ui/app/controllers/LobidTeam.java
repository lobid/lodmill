/* Copyright 2014 Pascal Christoph, hbz. Licensed under the Eclipse Public License 1.0 */

package controllers;

import models.Index;
import play.mvc.Controller;
import play.mvc.Result;

/**
 * Dataset controller. Implements path-style routes and `about` redirects.
 * 
 * @author Pascal Christoph (dr0i)
 */
public final class LobidTeam extends Controller {

	private LobidTeam() {
		/* No instantiation */
	}

	/**
	 * Redirects to {@link #apAbout(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result ap(final String format) {
		return redirect(routes.LobidTeam.apAbout(format));
	}

	/**
	 * Returns {@link #personAbout(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result apAbout(final String id, final String format) {
		return Collection.getId(id, format, Index.LOBID_TEAM);
	}

	/**
	 * Redirects to {@link #fsAboutString, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result fs(final String format) {
		return redirect(routes.LobidTeam.fsAbout(format));
	}

	/**
	 * Returns {@link #personAbout(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result fsAbout(final String id, final String format) {
		return Collection.getId(id, format, Index.LOBID_TEAM);
	}

	/**
	 * Redirects to {@link #pcAbout(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result pc(final String format) {
		return redirect(routes.LobidTeam.pcAbout(format));
	}

	/**
	 * Returns {@link #personAbout(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result pcAbout(final String id, final String format) {
		return Collection.getId(id, format, Index.LOBID_TEAM);
	}
}