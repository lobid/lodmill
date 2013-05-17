/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package models;

import java.util.ArrayList;
import java.util.List;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
			new InetSocketTransportAddress("10.1.2.111", 9300); // NOPMD
	/** The ElasticSearch cluster to use. */
	public static final String ES_CLUSTER_NAME = "es-lod-hydra";

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

	private static List<String> searchFields = Index.LOBID_RESOURCES.fields()
			.get("author");

	private static final Logger LOG = LoggerFactory
			.getLogger(Search.class);

	/**
	 * @param term The search term
	 * @param index The index to search (see {@link Index})
	 * @param category The search category (see {@link Index#fields()})
	 * @return The documents matching the given parameters
	 */
	public static List<Document> documents(final String term, final Index index,
			final String category) {
		validate(index, category);
		final String query = term.toLowerCase();
		QueryBuilder queryBuilder =
				index.constructQuery(query, Parameter.id(category));
		LOG.info("Using query: " + query);
		final SearchRequestBuilder requestBuilder =
				client.prepareSearch(index.id())
						.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
						.setQuery(queryBuilder);
		/* TODO: pass limit as a parameter */
		final SearchResponse response =
				requestBuilder.setFrom(0).setSize(50).setExplain(false).execute()
						.actionGet();
		final SearchHits hits = response.getHits();
		return asDocuments(query, hits);
	}

	private static void validate(final Index index, final String category) {
		if (index == null) {
			throw new IllegalArgumentException(String.format(
					"Invalid index ('%s') - valid indexes: %s", index, Index.values()));
		}
		searchFields = index.fields().get(category);
		if (searchFields == null) {
			throw new IllegalArgumentException(String.format(
					"Invalid type ('%s') for specified index ('%s') - valid types: %s",
					category, index, index.fields().keySet()));
		}
	}

	private static List<Document> asDocuments(final String query,
			final SearchHits hits) {
		final List<Document> res = new ArrayList<>();
		for (SearchHit hit : hits) {
			final Document document =
					new Document(hit.getId(), new String(hit.source()));
			res.add(Hit.of(hit, searchFields).process(query, document));
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
