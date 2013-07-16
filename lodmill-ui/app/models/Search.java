/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models;

import java.util.ArrayList;
import java.util.List;

import models.queries.AbstractIndexQuery;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
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
			new InetSocketTransportAddress("193.30.112.170", 9300); // NOPMD
	/** The ElasticSearch cluster to use. */
	public static final String ES_CLUSTER_NAME = "quaoar";

	private static Client productionClient = new TransportClient(
			ImmutableSettings.settingsBuilder().put("cluster.name", ES_CLUSTER_NAME)
					.build()).addTransportAddress(ES_SERVER);
	private static Client client = productionClient;

	/* TODO find a better way to inject the client for testing */

	/** @param newClient The new elasticsearch client to use. */
	public static void clientSet(Client newClient) {
		client = newClient;
	}

	/** Reset the elasticsearch client. */
	public static void clientReset() {
		client = productionClient;
	}

	/**
	 * @param term The search term
	 * @param index The index to search (see {@link Index})
	 * @param parameter The search parameter (see {@link Index#queries()} )
	 * @return The documents matching the given parameters
	 */
	public static List<Document> documents(final String term, final Index index,
			final Parameter parameter) {
		validate(index, parameter);
		final String query = term.toLowerCase();
		AbstractIndexQuery indexQuery = index.queries().get(parameter);
		final QueryBuilder queryBuilder = indexQuery.build(query);
		if (queryBuilder == null) {
			throw new IllegalStateException(String.format(
					"Could not construct query for term '%s', index '%s', param '%s'",
					query, index, parameter));
		}
		Logger.debug("Using query: " + queryBuilder);
		final SearchResponse response = search(index, queryBuilder);
		Logger.trace("Got response: " + response);
		final SearchHits hits = response.getHits();
		final List<Document> documents =
				asDocuments(query, hits, indexQuery.fields());
		Logger.debug(String.format("Got %s hits overall, created %s matching docs",
				hits.hits().length, documents.size()));
		return documents;
	}

	private static void validate(final Index index, final Parameter parameter) {
		if (index == null) {
			throw new IllegalArgumentException(String.format(
					"Invalid index ('%s') - valid indexes: %s", index, Index.values()));
		}
		if (!index.queries().containsKey(parameter)) {
			throw new IllegalArgumentException(String.format(
					"Invalid parameter ('%s') for specified index ('%s') - valid: %s",
					parameter, index, index.queries().keySet()));
		}
	}

	private static SearchResponse search(final Index index,
			QueryBuilder queryBuilder) {
		final SearchRequestBuilder requestBuilder =
				client.prepareSearch(index.id())
						.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
						.setQuery(queryBuilder);
		/* TODO: pass limit as a parameter */
		final SearchResponse response =
				requestBuilder.setFrom(0).setSize(50).setExplain(false).execute()
						.actionGet();
		return response;
	}

	private static List<Document> asDocuments(final String query,
			final SearchHits hits, List<String> searchFields) {
		final List<Document> res = new ArrayList<>();
		for (SearchHit hit : hits) {
			try {
				Hit hitEnum = Hit.of(hit, searchFields);
				final Document document =
						new Document(hit.getId(), new String(hit.source()));
				res.add(hitEnum.process(query, document));
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
