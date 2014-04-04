/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models.queries;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Queries on the GND index.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class Gnd {

	/**
	 * Query against all fields.
	 */
	public static class AllFieldsQuery extends AbstractIndexQuery {
		@Override
		public List<String> fields() {
			final List<String> suggestFields = new ArrayList<>();
			suggestFields.addAll(new PersonNameQuery().fields());
			suggestFields.addAll(new SubjectNameQuery().fields());
			suggestFields.addAll(Arrays.asList("_all"));
			return suggestFields;
		}

		@Override
		public QueryBuilder build(final String queryString) {
			return QueryBuilders.queryString(queryString).field(
					fields().get(fields().size() - 1));
		}
	}

	/**
	 * Query the GND index using a GND ID.
	 */
	public static class IdQuery extends AbstractIndexQuery {
		@Override
		public List<String> fields() {
			return Arrays
					.asList("@graph.http://d-nb.info/standards/elementset/gnd#gndIdentifier.@value");
		}

		@Override
		public QueryBuilder build(final String queryString) {
			return matchQuery(fields().get(0),
					queryString.replace("http://d-nb.info/gnd/", ""));
		}
	}

	/**
	 * Query the GND index using a person's name.
	 */
	public static class PersonNameQuery extends AbstractIndexQuery {

		@Override
		public List<String> fields() {
			return Arrays
					.asList(
							"@graph.http://d-nb.info/standards/elementset/gnd#preferredNameForThePerson.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#dateOfBirth.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#dateOfDeath.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#variantNameForThePerson.@value");
		}

		@Override
		public QueryBuilder build(final String queryString) {
			return searchAuthor(queryString);
		}
	}

	/**
	 * Query the GND index using a subject's name.
	 */
	public static class SubjectNameQuery extends AbstractIndexQuery {

		@Override
		public List<String> fields() {
			return Arrays
					.asList(
							"@graph.http://d-nb.info/standards/elementset/gnd#preferredNameForThePerson.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#variantNameForThePerson.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#preferredNameForTheConferenceOrEvent.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#variantNameForTheConferenceOrEvent.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#preferredNameForTheCorporateBody.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#variantNameForTheCorporateBody.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#preferredNameForTheFamily.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#variantNameForTheFamily.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#preferredNameForThePlaceOrGeographicName.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#variantNameForThePlaceOrGeographicName.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#preferredNameForTheSubjectHeading.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#variantNameForTheSubjectHeading.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#preferredNameForTheWork.@value",
							"@graph.http://d-nb.info/standards/elementset/gnd#variantNameForTheWork.@value");
		}

		@Override
		public QueryBuilder build(final String queryString) {
			return multiMatchQuery(queryString, fields().toArray(new String[] {}))
					.operator(Operator.AND);
		}

	}

}
