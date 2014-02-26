/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import models.queries.AbstractIndexQuery;
import models.queries.Gnd;
import models.queries.LobidItems;
import models.queries.LobidOrganisations;
import models.queries.LobidResources;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import play.Play;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * The different indices to use.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public enum Index {
	/***/
	LOBID_RESOURCES("lobid-resources", "json-ld-lobid",
			new ImmutableMap.Builder<Parameter, AbstractIndexQuery>()
					.put(Parameter.Q, new LobidResources.AllFieldsQuery())
					.put(Parameter.AUTHOR, new LobidResources.AuthorQuery())
					.put(Parameter.ID, new LobidResources.IdQuery())
					.put(Parameter.SUBJECT, new LobidResources.SubjectQuery())
					.put(Parameter.NAME, new LobidResources.NameQuery())
					.put(Parameter.SET, new LobidResources.SetQuery()).build()),
	/***/
	LOBID_ORGANISATIONS("lobid-organisations", "json-ld-lobid-orgs",
			new ImmutableMap.Builder<Parameter, AbstractIndexQuery>()/* @formatter:off */
					.put(Parameter.Q, new LobidOrganisations.AllFieldsQuery())
					.put(Parameter.NAME, new LobidOrganisations.NameQuery())
					.put(Parameter.ID, new LobidOrganisations.IdQuery()).build()),
	/***/
	GND("gnd", "json-ld-gnd",
			new ImmutableMap.Builder<Parameter, AbstractIndexQuery>()
					.put(Parameter.Q, new Gnd.AllFieldsQuery())
					.put(Parameter.NAME, new Gnd.PersonNameQuery())
					.put(Parameter.SUBJECT, new Gnd.SubjectNameQuery())
					.put(Parameter.ID, new Gnd.IdQuery()).build()),
	/***/
	LOBID_ITEMS("lobid-resources", "json-ld-lobid-item",
			new ImmutableMap.Builder<Parameter, AbstractIndexQuery>()
					.put(Parameter.Q, new LobidItems.AllFieldsQuery())
					.put(Parameter.NAME, new LobidItems.NameQuery())
					.put(Parameter.ID, new LobidItems.IdQuery()).build());/* @formatter:on */

	static final Config CONFIG = ConfigFactory.parseFile(
			new File("conf/application.conf")).resolve();
	private String id; // NOPMD
	private String type;
	private Map<Parameter, AbstractIndexQuery> queries;

	Index(final String name, final String type,
			final Map<Parameter, AbstractIndexQuery> params) {
		this.id = name;
		this.type = type;
		this.queries = params;
	}

	/** @return The Elasticsearch index name. */
	public String id() { // NOPMD
		return id + CONFIG.getString("application.index.suffix");
	}

	/** @return The Elasticsearch type name. */
	public String type() {
		return type;
	}

	/**
	 * @return The locations of the local and public JSON-LD contexts.
	 * @throws MalformedURLException If the constructed URL is malformed.
	 */
	public Pair<URL, String> context() throws MalformedURLException {
		final String path = "public/contexts";
		final String file = id + ".json";
		URL localContextResourceUrl =
				Play.application().resource("/" + path + "/" + file);
		if (localContextResourceUrl == null) // no app running, use plain local file
			localContextResourceUrl = new File(path, file).toURI().toURL();
		final String publicContextUrl =
				CONFIG.getString("application.url")
						+ controllers.routes.Assets.at("/" + path, file).url();
		return new ImmutablePair<>(localContextResourceUrl, publicContextUrl);
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