/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models.queries;

import static java.net.URLEncoder.encode;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Queries on the lobid-items index.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class LobidItems {
	/**
	 * Query the lobid-items index using an item ID.
	 */
	public static class IdQuery extends AbstractIndexQuery {

		@Override
		public List<String> fields() {
			return Arrays.asList("@graph.@id");
		}

		@Override
		public QueryBuilder build(final String queryString) {
			final String itemPrefix = "http://lobid.org/item/";
			final String shortId = queryString.replace(itemPrefix, "");
			try {
				/*
				 * The Lobid item IDs contain escaped entities, so we need to URL encode
				 * the ID. In particular, spaces are encode with '+', not '%2B', so we
				 * take care of that, too:
				 */
				final String encodedShortId =
						encode(shortId, "utf-8").replace("%2B", "+");
				return QueryBuilders.idsQuery("json-ld-lobid-item").ids(
						itemPrefix + encodedShortId);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			return null;
		}

	}

	/**
	 * Query the lobid-items index using an item name.
	 */
	public static class NameQuery extends AbstractIndexQuery {

		@Override
		public List<String> fields() {
			return Arrays.asList("@graph.http://purl.org/ontology/daia/label.@value");
		}

		@Override
		public QueryBuilder build(final String queryString) {
			return matchQuery(fields().get(0), queryString).operator(Operator.AND);
		}

	}

}
