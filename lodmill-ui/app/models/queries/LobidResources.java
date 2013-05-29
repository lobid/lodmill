/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models.queries;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Queries on the lobid-resources index.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class LobidResources {
	/**
	 * Query the lobid-resources index using a resource name.
	 */
	public static class NameQuery extends AbstractIndexQuery {

		@Override
		public List<String> fields() {
			return Arrays.asList("http://purl.org/dc/terms/title");
		}

		@Override
		public QueryBuilder build(final String queryString) {
			return matchQuery(fields().get(0), queryString);
		}

	}

	/**
	 * Query the lobid-resources index using a resource author.
	 */
	public static class AuthorQuery extends AbstractIndexQuery {
		@Override
		public List<String> fields() {
			return Arrays.asList(
					"http://purl.org/dc/elements/1.1/creator#preferredNameForThePerson",
					"http://purl.org/dc/elements/1.1/creator#dateOfBirth",
					"http://purl.org/dc/elements/1.1/creator#dateOfDeath",
					"http://purl.org/dc/elements/1.1/creator");
		}

		@Override
		public QueryBuilder build(final String queryString) {
			return searchAuthor(queryString);
		}
	}

	/**
	 * Query the lobid-resources index using a resource subject.
	 */
	public static class SubjectQuery extends AbstractIndexQuery {
		@Override
		public List<String> fields() {
			return Arrays.asList(/* @formatter:off*/
					"http://purl.org/dc/terms/subject#prefLabel",
					"http://purl.org/dc/terms/subject");/* @formatter:on */
		}

		@Override
		public QueryBuilder build(final String queryString) {
			final MatchQueryBuilder subjectLabelQuery =
					matchQuery(fields().get(0), queryString);
			final MatchQueryBuilder subjectIdQuery =
					matchQuery(fields().get(1) + ".@id",
							"http://d-nb.info/gnd/" + queryString).operator(Operator.AND);
			return boolQuery().should(subjectLabelQuery).should(subjectIdQuery);
		}
	}

	/**
	 * Query the lobid-resources index using a resource ID.
	 */
	public static class IdQuery extends AbstractIndexQuery {
		@Override
		public List<String> fields() {
			return Arrays.asList(/* @formatter:off*/
					"@id", 
					"http://purl.org/ontology/bibo/isbn13",
					"http://purl.org/ontology/bibo/isbn10"); /* @formatter:on */
		}

		@Override
		public QueryBuilder build(final String queryString) {
			final String fixedQuery = queryString.matches("ht[\\d]{9}") ?
			/* HT number -> URL (temp. until we have an HBZ-ID field) */
			"http://lobid.org/resource/" + queryString : queryString;
			return multiMatchQuery(fixedQuery, fields().toArray(new String[] {}));
		}
	}
}
