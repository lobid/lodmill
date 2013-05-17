/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
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
					.put("keyword", Arrays.asList("http://purl.org/dc/terms/subject"))
					.build()) {
		@Override
		QueryBuilder constructQuery(final String search, final Parameter parameter) {
			return parameter.query(search, this);
		}
	},
	/***/
	LOBID_ORGANISATIONS("lobid-orgs-index",
			new ImmutableMap.Builder<String, List<String>>().put("title",
					Arrays.asList("http://www.w3.org/2004/02/skos/core#prefLabel"))
					.build()) {
		@Override
		QueryBuilder constructQuery(final String search, final Parameter parameter) {
			return parameter.query(search, this);
		}
	},
	/***/
	GND(
			"gnd-index",
			new ImmutableMap.Builder<String, List<String>>()
					.put(
							"author",
							Arrays
									.asList(
											"http://d-nb.info/standards/elementset/gnd#preferredNameForThePerson",
											"http://d-nb.info/standards/elementset/gnd#dateOfBirth",
											"http://d-nb.info/standards/elementset/gnd#dateOfDeath",
											"http://d-nb.info/standards/elementset/gnd#variantNameForThePerson"))
					.build()) {
		@Override
		QueryBuilder constructQuery(final String search, final Parameter parameter) {
			return filterUndifferentiatedPersons(parameter.query(search, this));
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
}