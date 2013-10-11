/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models;

import java.util.Map;

import models.queries.AbstractIndexQuery;
import models.queries.Gnd;
import models.queries.LobidItems;
import models.queries.LobidOrganisations;
import models.queries.LobidResources;

import com.google.common.collect.ImmutableMap;

/**
 * The different indices to use.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public enum Index {
	/***/
	LOBID_RESOURCES("lobid-resources",
			new ImmutableMap.Builder<Parameter, AbstractIndexQuery>()
					.put(Parameter.Q, new LobidResources.AllFieldsQuery())
					.put(Parameter.AUTHOR, new LobidResources.AuthorQuery())
					.put(Parameter.ID, new LobidResources.IdQuery())
					.put(Parameter.SUBJECT, new LobidResources.SubjectQuery())
					.put(Parameter.NAME, new LobidResources.NameQuery())
					.put(Parameter.SET, new LobidResources.SetQuery()).build()),
	/***/
	LOBID_ORGANISATIONS("lobid-organisations",
			new ImmutableMap.Builder<Parameter, AbstractIndexQuery>()/* @formatter:off */
					.put(Parameter.Q, new LobidOrganisations.AllFieldsQuery())
					.put(Parameter.NAME, new LobidOrganisations.NameQuery())
					.put(Parameter.ID, new LobidOrganisations.IdQuery()).build()),
	/***/
	GND("gnd", new ImmutableMap.Builder<Parameter, AbstractIndexQuery>()
					.put(Parameter.Q, new Gnd.AllFieldsQuery())
					.put(Parameter.NAME, new Gnd.NameQuery())
					.put(Parameter.ID, new Gnd.IdQuery()).build()),
	/***/
	LOBID_ITEMS("lobid-items", new ImmutableMap.Builder<Parameter, AbstractIndexQuery>()
			.put(Parameter.Q, new LobidItems.AllFieldsQuery())
			.put(Parameter.NAME, new LobidItems.NameQuery())
			.put(Parameter.ID, new LobidItems.IdQuery()).build());/* @formatter:on */

	private String id; // NOPMD
	private Map<Parameter, AbstractIndexQuery> queries;

	Index(final String name, final Map<Parameter, AbstractIndexQuery> params) {
		this.id = name;
		this.queries = params;
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
	 * @return A mapping of parameters to corresponding queries for this index
	 */
	public Map<Parameter, AbstractIndexQuery> queries() {
		return queries;
	}

}