/* Copyright 2014 Pascal Christoph, hbz. Licensed under the Eclipse Public License 1.0 */

package controllers;

import java.util.ArrayList;
import java.util.List;

import models.Document;
import models.Index;
import models.Search;

import org.elasticsearch.action.get.GetResponse;

import play.Logger;
import play.mvc.Controller;
import play.mvc.Result;

/**
 * Dataset controller. Implements path-style routes and `about` redirects.
 * 
 * @author Pascal Christoph (dr0i)
 */
public final class Collection extends Controller {

	private Collection() {
		/* No instantiation */
	}

	/**
	 * Redirects to {@link #resourceAboutRPB(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result resourceRPB(final String format) {
		return redirect(routes.Collection.resourceAboutRPB(format));
	}

	/**
	 * Returns {@link #resourceAbout(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result resourceAboutRPB(final String id, final String format) {
		return getId(id, format, Index.LOBID_COLLECTIONS);
	}

	/**
	 * Redirects to {@link #resourceAboutNWBib(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result resourceNWBib(final String format) {
		return redirect(routes.Collection.resourceAboutNWBib(format));
	}

	/**
	 * Returns {@link #resourceAbout(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result resourceAboutNWBib(final String id, final String format) {
		return getId(id, format, Index.LOBID_COLLECTIONS);
	}

	/**
	 * Redirects to {@link #resourceAboutEdoweb(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result resourceEdoweb(final String format) {
		return redirect(routes.Collection.resourceAboutEdoweb(format));
	}

	/**
	 * Returns {@link #resourceAbout(String, String)}
	 */
	@SuppressWarnings("javadoc")
	public static Result resourceAboutEdoweb(final String id, final String format) {
		return getId(id, format, Index.LOBID_COLLECTIONS);
	}

	/**
	 * @param id the id to look up
	 * @param format TODO the serialization format as parameter
	 * @param dataIndex the index to be used
	 * @param indexType the type of the index to be used
	 */
	@SuppressWarnings("javadoc")
	public static Result getId(final String id, final String format,
			final Index index) {
		try {
			Logger.debug("Request:\n" + id);
			GetResponse response =
					Search.client.prepareGet(index.id(), index.type(), id).execute()
							.actionGet();
			Document doc = new Document(id, response.getSourceAsString(), index, "");
			List<Document> docs = new ArrayList<>();
			docs.add(doc);
			Logger.debug("Response:\n" + response.getSourceAsString());
			return !response.isExists() ? notFound() : Application.negotiateContent(
					docs, index, id, "");
		} catch (Exception x) {
			x.printStackTrace();
			return internalServerError(x.getMessage());
		}
	}
}