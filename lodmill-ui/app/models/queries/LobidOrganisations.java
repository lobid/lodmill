/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models.queries;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.index.query.QueryBuilder;

/**
 * Queries on the lobid-organisations index.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class LobidOrganisations {
	/**
	 * Query the lobid-organisations index using an organisation's ID.
	 */
	public static class IdQuery extends AbstractIndexQuery {

		@Override
		public List<String> fields() {
			return Arrays.asList("http://purl.org/dc/terms/identifier");
		}

		@Override
		public QueryBuilder build(String queryString) {
			return matchQuery(fields().get(0), queryString);
		}

	}

	/**
	 * Query the lobid-organisations index using an organisation's name.
	 */
	public static class NameQuery extends AbstractIndexQuery {

		@Override
		public List<String> fields() {
			return Arrays.asList("http://www.w3.org/2004/02/skos/core#prefLabel");
		}

		@Override
		public QueryBuilder build(final String queryString) {
			return matchQuery(fields().get(0), queryString);
		}

	}

}
