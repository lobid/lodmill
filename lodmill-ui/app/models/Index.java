/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import com.google.common.collect.ImmutableMap;

/**
 * The different indices to use.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public enum Index {
	/***/
	LOBID_RESOURCES(
			"lobid-index",
			new ImmutableMap.Builder<String, List<String>>()
					.put(
							"author",
							Arrays
									.asList(
											"http://purl.org/dc/elements/1.1/creator#preferredNameForThePerson",
											"http://purl.org/dc/elements/1.1/creator#dateOfBirth",
											"http://purl.org/dc/elements/1.1/creator#dateOfDeath",
											"http://purl.org/dc/elements/1.1/creator"))
					.put(
							"id",
							Arrays.asList("@id", "http://purl.org/ontology/bibo/isbn13",
									"http://purl.org/ontology/bibo/isbn10"))
					.put("subject", Arrays.asList("http://purl.org/dc/terms/subject"))
					.build()) {
		@Override
		QueryBuilder constructQuery(final String search, final Parameter parameter) {
			final List<String> searchFields = fields().get(parameter.id());
			switch (parameter) {
			case ID:
				QueryBuilder query;
				final String fixedQuery = search.matches("ht[\\d]{9}") ?
				/* HT number -> URL (temp. until we have an HBZ-ID field) */
				"http://lobid.org/resource/" + search : search;
				query =
						multiMatchQuery(fixedQuery, searchFields.toArray(new String[] {}));
				return query;
			case AUTHOR:
				return searchAuthor(search, searchFields);
			case SUBJECT:
				return nestedQuery(
						searchFields.get(0),
						matchQuery(searchFields.get(0) + ".@id", "http://d-nb.info/gnd/"
								+ search));
			case NAME:
				throw new IllegalArgumentException(String.format(
						"Not implemented: parameter type '%s' for index '%s'", parameter,
						this));
			default:
				throw new IllegalArgumentException(String.format(
						"Unsupported parameter type '%s' for index '%s'", parameter, this));
			}
		}
	},
	/***/
	LOBID_ORGANISATIONS("lobid-orgs-index",
			new ImmutableMap.Builder<String, List<String>>().put("name",
					Arrays.asList("http://www.w3.org/2004/02/skos/core#prefLabel"))
					.build()) {
		@Override
		QueryBuilder constructQuery(final String search, final Parameter parameter) {
			switch (parameter) {
			case NAME:
				return matchQuery(fields().get(parameter.id()).get(0), search);
			case ID:
				throw new IllegalArgumentException(String.format(
						"Not implemented: parameter type '%s' for index '%s'", parameter,
						this));
			default:
				throw new IllegalArgumentException(String.format(
						"Unsupported parameter type '%s' for index '%s'", parameter, this));
			}

		}
	},
	/***/
	GND(
			"gnd-index",
			new ImmutableMap.Builder<String, List<String>>()
					.put(
							"name",
							Arrays
									.asList(
											"http://d-nb.info/standards/elementset/gnd#preferredNameForThePerson",
											"http://d-nb.info/standards/elementset/gnd#dateOfBirth",
											"http://d-nb.info/standards/elementset/gnd#dateOfDeath",
											"http://d-nb.info/standards/elementset/gnd#variantNameForThePerson"))
					.build()) {
		@Override
		QueryBuilder constructQuery(final String search, final Parameter parameter) {
			switch (parameter) {
			case NAME:
				return filterUndifferentiatedPersons(searchAuthor(search,
						fields().get(parameter.id())));
			case ID:
				throw new IllegalArgumentException(String.format(
						"Not implemented: parameter type '%s' for index '%s'", parameter,
						this));
			default:
				throw new IllegalArgumentException(String.format(
						"Unsupported parameter type '%s' for index '%s'", parameter, this));
			}
		}

		private QueryBuilder filterUndifferentiatedPersons(final QueryBuilder query) {
			/* TODO: set up a filters map if we have any more such cases */
			return boolQuery().must(query).must(
					matchQuery("@type",
							"http://d-nb.info/standards/elementset/gnd#DifferentiatedPerson")
							.operator(Operator.AND));
		}
	};

	private Map<String, List<String>> fields;
	private String id; // NOPMD

	Index(final String name, final Map<String, List<String>> fields) {
		this.id = name;
		this.fields = fields;
	}

	/** @return The Elasticsearch index name. */
	public String id() { // NOPMD
		return id;
	}

	/**
	 * @param id The Elasticsearch index name
	 * @return The index enum element with the given id
	 * @throws IllegalArgumentException if there is no index with the id
	 */
	public static Index id(final String id) { // NOPMD
		for (Index index : values()) {
			if (index.id().equals(id)) {
				return index;
			}
		}
		throw new IllegalArgumentException("No such index: " + id);
	}

	/**
	 * @return A mapping of categories to corresponding fields
	 */
	public Map<String, List<String>> fields() {
		return fields;
	}

	abstract QueryBuilder constructQuery(String query, Parameter parameter);

	private static QueryBuilder searchAuthor(final String search,
			final List<String> fields) {
		QueryBuilder query;
		final String lifeDates = "\\((\\d+)-(\\d*)\\)";
		final Matcher lifeDatesMatcher =
				Pattern.compile("[^(]+" + lifeDates).matcher(search);
		if (lifeDatesMatcher.find()) {
			query = createAuthorQuery(lifeDates, search, lifeDatesMatcher, fields);
		} else if (search.matches("\\d+")) {
			query = matchQuery(fields.get(3) + ".@id", search);
		} else {
			query = nameMatchQuery(search, fields);
		}
		return query;
	}

	private static QueryBuilder createAuthorQuery(final String lifeDates,
			final String search, final Matcher matcher, final List<String> fields) {
		/* Search name in name field and birth in birth field: */
		final BoolQueryBuilder birthQuery =
				boolQuery().must(
						nameMatchQuery(search.replaceAll(lifeDates, "").trim(), fields))
						.must(matchQuery(fields.get(1), matcher.group(1)));
		return matcher.group(2).equals("") ? birthQuery :
		/* If we have one, search death in death field: */
		birthQuery.must(matchQuery(fields.get(2), matcher.group(2)));
	}

	private static QueryBuilder nameMatchQuery(final String search,
			final List<String> fields) {
		final MultiMatchQueryBuilder query =
				multiMatchQuery(search, fields.get(0)).operator(Operator.AND);
		return fields.size() > 3 ? query.field(fields.get(3)) : query;
	}
}