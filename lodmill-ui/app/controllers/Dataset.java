/* Copyright 2014 Pascal Christoph, hbz. Licensed under the Eclipse Public License 1.0 */

package controllers;

import models.Search;

import org.elasticsearch.action.get.GetResponse;

import play.Logger;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

/**
 * Dataset controller. Implements path-style routes and `about` redirects.
 * 
 * @author Pascal Christoph (dr0i)
 */
public final class Dataset extends Controller {
	// TODO don't hardwire index name to switch easily between staging and
	// production
	static final String DATA_INDEX = "lobid-provenance";
	static final String INDEX_TYPE = "json-ld-lobid-provenance";

	private Dataset() {
		/* No instantiation */
	}

	/**
	 * Redirects to {@link #resourceAboutRPB(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result resourceRPB(final String format) {
		return redirect(routes.Dataset.resourceAboutRPB(format));
	}

	/**
	 * Returns {@link #resourceAbout(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result resourceAboutRPB(final String id, final String format) {
		return getId(id, format, DATA_INDEX, INDEX_TYPE);
	}

	/**
	 * Redirects to {@link #resourceAboutNWBib(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result resourceNWBib(final String format) {
		return redirect(routes.Dataset.resourceAboutNWBib(format));
	}

	/**
	 * Returns {@link #resourceAbout(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result resourceAboutNWBib(final String id, final String format) {
		return getId(id, format, DATA_INDEX, INDEX_TYPE);
	}

	/**
	 * Redirects to {@link #resourceAboutEdoweb(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result resourceEdoweb(final String format) {
		return redirect(routes.Dataset.resourceAboutEdoweb(format));
	}

	/**
	 * Returns {@link #resourceAbout(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result resourceAboutEdoweb(final String id, final String format) {
		return getId(id, format, DATA_INDEX, INDEX_TYPE);
	}

	/**
	 * @param id the id to look up
	 * @param format TODO : format, content negotiation
	 * @param dataIndex the index to be used
	 * @param indexType the type of the index to be used
	 */
	@SuppressWarnings("javadoc")
	public static Result getId(final String id, final String format,
			final String dataIndex, final String indexType) {
		try {
			Logger.debug("Request:\n" + id);
			GetResponse response =
					Search.client.prepareGet(dataIndex, indexType, id).execute()
							.actionGet();
			Logger.debug("Response:\n" + response.getSourceAsString());
			return !response.isExists() ? notFound() : ok(Json.parse(response
					.getSourceAsString()));
		} catch (Exception x) {
			x.printStackTrace();
			return internalServerError(x.getMessage());
		}
	}
}