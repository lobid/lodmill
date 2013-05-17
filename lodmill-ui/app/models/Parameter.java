/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Parameters for API requests.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public enum Parameter {
	/***/
	ID("id") {
		@Override
		public QueryBuilder query(final String search, final Index index) {
			QueryBuilder query;
			final String fixedQuery = search.matches("ht[\\d]{9}") ?
			/* HT number -> URL (temp. until we have an HBZ-ID field) */
			"http://lobid.org/resource/" + search : search;
			query =
					multiMatchQuery(fixedQuery, fields(index).toArray(new String[] {}));
			return query;
		}
	},
	/***/
	NAME("name") {
		@Override
		public QueryBuilder query(final String search, final Index index) {
			return null; /* TODO: odd, this works */
		}
	},
	/***/
	TITLE("title") { /* TODO: use 'name' for organisations */
		@Override
		public QueryBuilder query(final String search, final Index index) {
			return null; /* TODO: odd, this works */
		}
	},
	/***/
	AUTHOR("author") {
		@Override
		public QueryBuilder query(final String search, final Index index) {
			QueryBuilder query;
			final String lifeDates = "\\((\\d+)-(\\d*)\\)";
			final Matcher lifeDatesMatcher =
					Pattern.compile("[^(]+" + lifeDates).matcher(search);
			if (lifeDatesMatcher.find()) {
				query = createAuthorQuery(lifeDates, search, lifeDatesMatcher, index);
			} else if (search.matches("\\d+")) {
				query = matchQuery(fields(index).get(3) + ".@id", search);
			} else {
				query = nameMatchQuery(search, index);
			}
			return query;
		}

		private QueryBuilder createAuthorQuery(final String lifeDates,
				final String search, final Matcher matcher, final Index index) {
			/* Search name in name field and birth in birth field: */
			final BoolQueryBuilder birthQuery =
					boolQuery().must(
							nameMatchQuery(search.replaceAll(lifeDates, "").trim(), index))
							.must(matchQuery(fields(index).get(1), matcher.group(1)));
			return matcher.group(2).equals("") ? birthQuery :
			/* If we have one, search death in death field: */
			birthQuery.must(matchQuery(fields(index).get(2), matcher.group(2)));
		}

		private QueryBuilder nameMatchQuery(final String search, final Index index) {
			final MultiMatchQueryBuilder query =
					multiMatchQuery(search, index.fields().get(id()).get(0)).operator(
							Operator.AND);
			return fields(index).size() > 3 ? query.field(fields(index).get(3))
					: query;
		}
	},
	/***/
	SUBJECT("keyword") {
		@Override
		public QueryBuilder query(final String search, final Index index) {
			return nestedQuery(
					fields(index).get(0),
					matchQuery(fields(index).get(0) + ".@id", "http://d-nb.info/gnd/"
							+ search));
		}
	};
	private String id; // NOPMD

	/**
	 * @return The parameter id (the string passed to the API)
	 */
	public String id() { // NOPMD
		return id;
	}

	/**
	 * @param id The parameter id (the string passed to the API)
	 * @return The parameter object corresponding to the id
	 */
	public static Parameter id(final String id) {// NOPMD
		for (Parameter parameter : values()) {
			if (parameter.id().equals(id)) {
				return parameter;
			}
		}
		throw new IllegalArgumentException("No such parameter: " + id);
	}

	abstract QueryBuilder query(String search, Index index);

	List<String> fields(final Index index) {
		return index.fields().get(id);
	}

	Parameter(final String id) { // NOPMD
		this.id = id;
	}
}
