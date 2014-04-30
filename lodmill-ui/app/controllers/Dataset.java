/* Copyright 2014 Pascal Christoph, hbz. Licensed under the Eclipse Public License 1.0 */

package controllers;

import models.Search;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

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
		return resourceAbout(id, format, DATA_INDEX, INDEX_TYPE);
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
		return resourceAbout(id, format, DATA_INDEX, INDEX_TYPE);
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
		return resourceAbout(id, format, DATA_INDEX, INDEX_TYPE);
	}

	/**
	 * @param id the id to look up
	 * @param format TODO : format, content negotiation
	 * @param dataIndex the index to be used
	 * @param indexType the type of the index to be used
	 */
	@SuppressWarnings("javadoc")
	public static Result resourceAbout(final String id, final String format,
			final String dataIndex, final String indexType) {
		try {
			MatchQueryBuilder query = QueryBuilders.matchQuery("_id", id);
			SearchResponse response = search(dataIndex, query, indexType);
			boolean found = response.getHits().getTotalHits() > 0;
			return !found ? notFound() : ok(Json.parse("["
					+ response.getHits().getAt(0).getSourceAsString() + "]"));
		} catch (Exception x) {
			x.printStackTrace();
			return internalServerError(x.getMessage());
		}
	}

	private static SearchResponse search(String index, QueryBuilder queryBuilder,
			String type) {
		SearchRequestBuilder requestBuilder =
				Search.client.prepareSearch(index)
						.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
						.setQuery(queryBuilder).setTypes(type);
		Logger.debug("Request:\n" + requestBuilder);
		SearchResponse response =
				requestBuilder.setExplain(false).execute().actionGet();
		Logger.debug("Response:\n" + response);
		return response;
	}
}