/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

import java.util.ArrayList;
import java.util.List;

import models.queries.AbstractIndexQuery;
import models.queries.LobidItems;
import models.queries.LobidResources;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import play.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Search documents in an ElasticSearch index.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class Search {

	/** The ElasticSearch server to use. */
	public static final InetSocketTransportAddress ES_SERVER =
			new InetSocketTransportAddress(
					Index.CONFIG.getString("application.es.server"),
					Index.CONFIG.getInt("application.es.port"));
	/** The ElasticSearch cluster to use. */
	public static final String ES_CLUSTER_NAME = Index.CONFIG
			.getString("application.es.cluster");

	private static Client productionClient = new TransportClient(
			ImmutableSettings.settingsBuilder().put("cluster.name", ES_CLUSTER_NAME)
					.build()).addTransportAddress(ES_SERVER);
	private static Client client = productionClient;

	/** Required: */
	private String term;
	private Index index;
	private Parameter parameter;

	/** Optional: */
	private String field = "";
	private String owner = "";
	private String set = "";
	private int size = 50;
	private int from = 0;
	private String type = "";

	/* TODO find a better way to inject the client for testing */

	/**
	 * @param term The search term
	 * @param index The index to search (see {@link Index})
	 * @param parameter The search parameter (see {@link Index#queries()} )
	 */
	public Search(final String term, final Index index, final Parameter parameter) {
		this.term = term;
		this.index = index;
		this.parameter = parameter;
	}

	/** @param newClient The new elasticsearch client to use. */
	public static void clientSet(Client newClient) {
		client = newClient;
	}

	/** Reset the elasticsearch client. */
	public static void clientReset() {
		client = productionClient;
	}

	/**
	 * Execute the search and return its results.
	 * 
	 * @return The documents matching this search
	 */
	public List<Document> documents() {
		validateSearchParameters();
		final AbstractIndexQuery indexQuery = index.queries().get(parameter);
		final QueryBuilder queryBuilder = createQuery(indexQuery);
		Logger.debug("Using query: " + queryBuilder);
		final SearchResponse response = search(queryBuilder);
		Logger.debug("Got response: " + response);
		final SearchHits hits = response.getHits();
		final List<Document> documents = asDocuments(hits, indexQuery.fields());
		Logger.debug(String.format("Got %s hits overall, created %s matching docs",
				hits.hits().length, documents.size()));
		return documents;
	}

	/**
	 * Optional: specify a field to pick from the full result
	 * 
	 * @param resultField The field to return as the result
	 * @return this search object (for chaining)
	 */
	public Search field(final String resultField) {
		this.field = resultField;
		return this;
	}

	/**
	 * Optional: specify a resource owner
	 * 
	 * @param resourceOwner An ID for the owner of requested resources
	 * @return this search object (for chaining)
	 */
	public Search owner(final String resourceOwner) {
		this.owner = resourceOwner;
		return this;
	}

	/**
	 * Optional: specify a resource set
	 * 
	 * @param resourceSet An ID for the set the requested resources should be in
	 * @return this search object (for chaining)
	 */
	public Search set(final String resourceSet) {
		this.set = resourceSet;
		return this;
	}

	/**
	 * Optional: specify the page size
	 * 
	 * @param pageFrom The start index of the result set
	 * @param pageSize The size of the result set
	 * @return this search object (for chaining)
	 */
	public Search page(final int pageFrom, final int pageSize) {
		this.from = pageFrom;
		this.size = pageSize;
		return this;
	}

	/**
	 * Optional: specify a type
	 * 
	 * @param resourceType The type of the requested resources
	 * @return this search object (for chaining)
	 */
	public Search type(final String resourceType) {
		this.type = resourceType;
		return this;
	}

	private QueryBuilder createQuery(final AbstractIndexQuery indexQuery) {
		QueryBuilder queryBuilder = indexQuery.build(term);
		if (!owner.isEmpty()) {
			final QueryBuilder itemQuery = new LobidItems.OwnerQuery().build(owner);
			queryBuilder = boolQuery().must(queryBuilder).must(itemQuery);
		}
		if (!set.isEmpty()) {
			final QueryBuilder setQuery = new LobidResources.SetQuery().build(set);
			queryBuilder = boolQuery().must(queryBuilder).must(setQuery);
		}
		if (!type.isEmpty()) {
			final QueryBuilder typeQuery =
					matchQuery("@graph.@type", type).operator(
							MatchQueryBuilder.Operator.AND);
			queryBuilder = boolQuery().must(queryBuilder).must(typeQuery);
		}
		if (queryBuilder == null)
			throw new IllegalStateException(String.format(
					"Could not construct query for term '%s', owner '%s'", term, owner));
		return queryBuilder;
	}

	private void validateSearchParameters() {
		if (index == null) {
			throw new IllegalArgumentException(String.format(
					"Invalid index ('%s') - valid indexes: %s", index, Index.values()));
		}
		if (!index.queries().containsKey(parameter)) {
			throw new IllegalArgumentException(String.format(
					"Invalid parameter ('%s') for specified index ('%s') - valid: %s",
					parameter, index, index.queries().keySet()));
		}
		if (from < 0) {
			throw new IllegalArgumentException("Parameter 'from' must be positive");
		}
		if (size > 100) {
			throw new IllegalArgumentException("Parameter 'size' must be <= 100");
		}
	}

	private SearchResponse search(final QueryBuilder queryBuilder) {
		final SearchRequestBuilder requestBuilder =
				client.prepareSearch(index.id())
						.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
						.setQuery(queryBuilder)
						.setFilter(FilterBuilders.typeFilter(index.type()));
		final SearchResponse response =
				requestBuilder.setFrom(from).setSize(size).setExplain(false).execute()
						.actionGet();
		return response;
	}

	private List<Document> asDocuments(final SearchHits hits,
			final List<String> searchFields) {
		final List<Document> res = new ArrayList<>();
		for (SearchHit hit : hits) {
			try {
				Hit hitEnum = Hit.of(hit, searchFields);
				final Document document =
						new Document(hit.getId(), new String(hit.source()), index, field);
				res.add(hitEnum.process(term, document));
			} catch (IllegalArgumentException e) {
				Logger.error(e.getMessage(), e);
			}
		}
		final Predicate<Document> predicate = new Predicate<Document>() {
			@Override
			public boolean apply(final Document doc) {
				return doc.matchedField != null;
			}
		};
		return ImmutableList.copyOf(Iterables.filter(res, predicate));
	}
}
