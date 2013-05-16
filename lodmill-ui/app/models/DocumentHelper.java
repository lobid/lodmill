/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package models;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Documents returned from the ElasticSearch index.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class DocumentHelper {

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
			.getLogger(DocumentHelper.class);

	/**
	 * @param term The search term
	 * @param index The index to search (see {@link Index})
	 * @param category The search category (see {@link Index#fields()})
	 * @return The documents matching the given parameters
	 */
	public static List<Document> search(final String term, final Index index,
			final String category) {
		validate(index, category);
		final String query = term.toLowerCase();
		final SearchRequestBuilder requestBuilder =
				client.prepareSearch(index.id())
						.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
						.setQuery(constructQuery(index, query, category));
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

	private static QueryBuilder constructQuery(final Index index,
			final String search, final String category) {
		QueryBuilder query = Parameter.id(category).query(search, index);
		if (index.equals(Index.GND)) {
			query = filterUndifferentiatedPersons(query);
		}
		LOG.debug("Using query: " + query);
		return query;
	}

	private static QueryBuilder filterUndifferentiatedPersons(QueryBuilder query) {
		/* TODO: set up a filters map if we have any more such cases */
		return boolQuery().must(query).must(
				matchQuery("@type",
						"http://d-nb.info/standards/elementset/gnd#DifferentiatedPerson")
						.operator(Operator.AND));
	}

	private static List<Document> asDocuments(final String query,
			final SearchHits hits) {
		final List<Document> res = new ArrayList<>();
		for (SearchHit hit : hits) {
			final Document document =
					new Document(hit.getId(), new String(hit.source()));
			withMatchedField(query, hit, document);
			res.add(document);
		}
		final Predicate<Document> predicate = new Predicate<Document>() {
			@Override
			public boolean apply(final Document doc) {
				return doc.matchedField != null;
			}
		};
		return ImmutableList.copyOf(Iterables.filter(res, predicate));
	}

	private static void withMatchedField(final String query, final SearchHit hit,
			final Document document) {
		final Object matchedField = firstExisting(hit);
		/* TODO: replace with polymorphic dispatch */
		if (matchedField instanceof List) {
			processList(query, document, matchedField);
		} else if (searchFields.get(0).contains("preferredNameForThePerson")) {
			final Object birth = hit.getSource().get(searchFields.get(1));
			final Object death = hit.getSource().get(searchFields.get(2));
			if (birth == null) {
				document.matchedField = matchedField.toString();
			} else {
				final String format =
						String.format("%s (%s-%s)", matchedField.toString(),
								birth.toString(), death == null ? "" : death.toString());
				document.matchedField = format;
			}
		} else if (matchedField instanceof String) {
			document.matchedField = matchedField.toString();
		} else if (matchedField instanceof Map) {
			@SuppressWarnings("unchecked")
			Map<String, Object> map = (Map<String, Object>) matchedField;
			processMaps(query, document, Arrays.asList(map));
		}
	}

	private static Object firstExisting(final SearchHit hit) {
		for (String field : searchFields) {
			if (hit.getSource().containsKey(field)) {
				return hit.getSource().get(field);
			}
		}
		return null;
	}

	private static void processList(final String query, final Document document,
			final Object matchedField) {
		List<?> list = (List<?>) matchedField;
		if (list.get(0) instanceof String) {
			@SuppressWarnings("unchecked")
			final List<String> strings = (List<String>) matchedField;
			document.matchedField = firstMatching(query, strings);
		} else if (list.get(0) instanceof Map) {
			@SuppressWarnings("unchecked")
			final List<Map<String, Object>> maps =
					(List<Map<String, Object>>) matchedField;
			processMaps(query, document, maps);
		}
	}

	private static void processMaps(final String query, final Document document,
			final List<Map<String, Object>> maps) {
		for (Map<String, Object> map : maps) {
			if (map.get("@id").toString().contains(query)) {
				document.matchedField = map.get("@id").toString();
				break;
			}
		}
	}

	private static String firstMatching(final String query,
			final List<String> list) {
		final Predicate<String> predicate = new Predicate<String>() {
			@Override
			public boolean apply(final String string) {
				return string.toLowerCase().contains(query);
			}
		};
		return Iterables.tryFind(list, predicate).orNull();
	}
}
