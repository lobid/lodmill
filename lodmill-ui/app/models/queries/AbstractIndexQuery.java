/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models.queries;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Superclass for queries on different indexes.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public abstract class AbstractIndexQuery {
	/**
	 * @return The index fields used by this query type
	 */
	public abstract List<String> fields();

	/**
	 * @param queryString The query string
	 * @return A query builder for this query type and the given query string
	 */
	public abstract QueryBuilder build(String queryString);

	/* Some common author query stuff used both for gnd and lobid-resources: */

	QueryBuilder searchAuthor(final String search) {
		QueryBuilder query;
		final String lifeDates = "\\((\\d+)-(\\d*)\\)";
		final Matcher lifeDatesMatcher =
				Pattern.compile("[^(]+" + lifeDates).matcher(search);
		if (lifeDatesMatcher.find()) {
			query = createAuthorQuery(lifeDates, search, lifeDatesMatcher);
		} else if (search.matches("(http://d-nb\\.info/gnd/)?\\d+")) {
			final String term =
					search.startsWith("http") ? search : "http://d-nb.info/gnd/" + search;
			query = matchQuery(fields().get(3) + ".@id", term);
		} else {
			query = nameMatchQuery(search);
		}
		return query;
	}

	private QueryBuilder createAuthorQuery(final String lifeDates,
			final String search, final Matcher matcher) {
		/* Search name in name field and birth in birth field: */
		final BoolQueryBuilder birthQuery =
				boolQuery().must(
						nameMatchQuery(search.replaceAll(lifeDates, "").trim())).must(
						matchQuery(fields().get(1), matcher.group(1)));
		return matcher.group(2).equals("") ? birthQuery :
		/* If we have one, search death in death field: */
		birthQuery.must(matchQuery(fields().get(2), matcher.group(2)));
	}

	private QueryBuilder nameMatchQuery(final String search) {
		final MultiMatchQueryBuilder query =
				multiMatchQuery(search, fields().get(0)).operator(Operator.AND);
		return fields().size() > 3 ? query.field(fields().get(3)) : query;
	}
}
