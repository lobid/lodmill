/* Copyright 2014 Pascal Christoph, hbz. Licensed under the Eclipse Public License 1.0 */

package models.queries;

import java.util.List;

import org.elasticsearch.index.query.QueryBuilder;

/**
 * Dummy queries.
 * 
 * @author Pascal Christoph (dr0i)
 */
public class Dummy {

	/**
	 * Dummy query.
	 */
	public static class IdQuery extends AbstractIndexQuery {

		@Override
		public List<String> fields() {
			return null;
		}

		@Override
		public QueryBuilder build(final String queryString) {
			return null;
		}

	}
}
