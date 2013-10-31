/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models.queries;

import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Queries on the lobid-organisations index.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class LobidOrganisations {

	/**
	 * Query against all fields.
	 */
	public static class AllFieldsQuery extends AbstractIndexQuery {
		@Override
		public List<String> fields() {
			final List<String> searchFields = new ArrayList<>(Arrays.asList("_all"));
			final List<String> suggestFields = new NameQuery().fields();
			searchFields.addAll(suggestFields);
			return searchFields;
		}

		@Override
		public QueryBuilder build(final String queryString) {
			return QueryBuilders.queryString(queryString).field(fields().get(0));
		}
	}

	/**
	 * Query the lobid-organisations index using an organisation's ID.
	 */
	public static class IdQuery extends AbstractIndexQuery {

		@Override
		public List<String> fields() {
			return Arrays.asList("@graph.http://purl.org/lobid/lv#isil.@value");
		}

		@Override
		public QueryBuilder build(final String queryString) {
			final String itemPrefix = "http://lobid.org/organisation/";
			return QueryBuilders.idsQuery("json-ld-lobid-orgs").ids(
					itemPrefix + queryString.replace(itemPrefix, ""));
		}

	}

	/**
	 * Query the lobid-organisations index using an organisation's name.
	 */
	public static class NameQuery extends AbstractIndexQuery {

		@Override
		public List<String> fields() {
			return Arrays.asList("@graph.http://xmlns.com/foaf/0.1/name.@value",
					"@graph.http://www.w3.org/2004/02/skos/core#prefLabel.@value");
		}

		@Override
		public QueryBuilder build(final String queryString) {
			return multiMatchQuery(queryString, fields().toArray(new String[] {}))
					.operator(Operator.AND);
		}

	}

}
