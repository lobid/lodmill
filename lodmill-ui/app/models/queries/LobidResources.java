/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models.queries;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Queries on the lobid-resources index.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class LobidResources {

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
	 * Query the lobid-resources index using a resource set.
	 */
	public static class SetQuery extends AbstractIndexQuery {

		@Override
		public List<String> fields() {
			return Arrays.asList("@graph.http://purl.org/dc/terms/isPartOf.@id");
		}

		@Override
		public QueryBuilder build(final String queryString) {
			final String prefix = "http://lobid.org/resource/";
			return matchQuery(fields().get(0),
					prefix + queryString.replace(prefix, "")).operator(Operator.AND);
		}

	}

	/**
	 * Query the lobid-resources index using a resource name.
	 */
	public static class NameQuery extends AbstractIndexQuery {

		@Override
		public List<String> fields() {
			return Arrays.asList("@graph.http://purl.org/dc/terms/title.@value",
					"http://purl.org/dc/terms/alternative");
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
			return Arrays
					.asList(
							"@graph.http://d-nb.info/standards/elementset/gnd#preferredNameForThePerson.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#dateOfBirth.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#dateOfDeath.@value",
							"@graph.http://purl.org/dc/terms/creator");
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
					"@graph.http://www.w3.org/2004/02/skos/core#prefLabel.@value",
					"@graph.http://purl.org/dc/terms/subject");/* @formatter:on */
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
					"@graph.@id", // hbz ID
					"@graph.http://purl.org/ontology/bibo/isbn13.@value",
					"@graph.http://purl.org/ontology/bibo/isbn.@value",
					"@graph.http://purl.org/ontology/bibo/issn.@value",
					"@graph.http://purl.org/lobid/lv#urn.@value"); /* @formatter:on */
		}

		@Override
		public QueryBuilder build(final String queryString) {
			return multiMatchQuery(lobidResourceQueryString(queryString),
					fields().toArray(new String[] {})).operator(Operator.AND);
		}
	}

	private static String lobidResourceQueryString(final String queryString) {
		final String hbzId = "\\p{L}+\\d+(-.+)?";
		return queryString.matches(hbzId) ? "http://lobid.org/resource/"
				+ queryString : queryString;
	}
}
