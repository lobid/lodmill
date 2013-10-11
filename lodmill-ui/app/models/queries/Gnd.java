/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models.queries;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

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
			final List<String> suggestFields =
					new ArrayList<>(new NameQuery().fields());
			final List<String> searchFields = Arrays.asList("_all");
			suggestFields.addAll(searchFields);
			return suggestFields;
		}

		@Override
		public QueryBuilder build(final String queryString) {
			return NameQuery.filterUndifferentiatedPersons(QueryBuilders.queryString(
					queryString).field(fields().get(fields().size() - 1)));
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
			return matchQuery(fields().get(0), queryString);
		}
	}

	/**
	 * Query the GND index using a person's name.
	 */
	public static class NameQuery extends AbstractIndexQuery {

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
			return filterUndifferentiatedPersons(searchAuthor(queryString));
		}

		static QueryBuilder filterUndifferentiatedPersons(final QueryBuilder query) {
			return boolQuery().must(query).must(
					matchQuery("@graph.@type",
							"http://d-nb.info/standards/elementset/gnd#DifferentiatedPerson")
							.operator(Operator.AND));
		}

	}

}
