/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package models;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.json.simple.JSONValue;
import org.lobid.lodmill.JsonLdConverter;
import org.lobid.lodmill.JsonLdConverter.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.hp.hpl.jena.shared.BadURIException;

import controllers.Application.Index;

/**
 * Documents returned from the ElasticSearch index.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class Document {

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

	/** A mapping index names to categories to search fields. */
	public static ImmutableMap<Index, Map<String, List<String>>> searchFieldsMap =
			new ImmutableMap.Builder<Index, Map<String, List<String>>>()
					.put(
							Index.LOBID_RESOURCES,
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
											Arrays.asList("@id",
													"http://purl.org/ontology/bibo/isbn13",
													"http://purl.org/ontology/bibo/isbn10"))
									.put("keyword",
											Arrays.asList("http://purl.org/dc/terms/subject"))
									.build())
					.put(
							Index.GND,
							new ImmutableMap.Builder<String, List<String>>()
									.put(
											"author",
											Arrays
													.asList(
															"http://d-nb.info/standards/elementset/gnd#preferredNameForThePerson",
															"http://d-nb.info/standards/elementset/gnd#dateOfBirth",
															"http://d-nb.info/standards/elementset/gnd#dateOfDeath",
															"http://d-nb.info/standards/elementset/gnd#variantNameForThePerson"))
									.build())
					.put(
							Index.LOBID_ORGANISATIONS,
							new ImmutableMap.Builder<String, List<String>>().put(
									"title",
									Arrays
											.asList("http://www.w3.org/2004/02/skos/core#prefLabel"))
									.build()).build();

	private static List<String> searchFields = searchFieldsMap.get(
			Index.LOBID_RESOURCES).get("author");

	private transient String matchedField;
	private transient String source;
	private transient String id; // NOPMD

	/** @return The document ID. */
	public String getId() {
		return id;
	}

	/** @return The JSON source for this document. */
	public String getSource() {
		return source;
	}

	/** @return The field that matched the query. */
	public String getMatchedField() {
		return matchedField;
	}

	private static final Logger LOG = LoggerFactory.getLogger(Document.class);

	/**
	 * @param id The document ID
	 * @param source The document JSON source
	 */
	public Document(final String id, final String source) { // NOPMD
		this.id = id;
		this.source = source;
	}

	/**
	 * @param format The RDF serialization format to represent this document as
	 * @return This documents, in the given RDF format
	 */
	public String as(final Format format) { // NOPMD
		final JsonLdConverter converter = new JsonLdConverter(format);
		final String json = JSONValue.toJSONString(JSONValue.parse(source));
		String result = "";
		try {
			result = converter.toRdf(json);
		} catch (BadURIException x) {
			LOG.error(x.getMessage(), x);
		}
		return result;
	}

	/**
	 * @param term The search term
	 * @param index The index to search (see {@link #searchFieldsMap})
	 * @param category The search category (see {@link #searchFieldsMap})
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
		if (searchFieldsMap.get(index) == null) {
			throw new IllegalArgumentException(String.format(
					"Invalid index ('%s') - valid indexes: %s", index,
					searchFieldsMap.keySet()));
		}
		searchFields = searchFieldsMap.get(index).get(category);
		if (searchFields == null) {
			throw new IllegalArgumentException(String.format(
					"Invalid type ('%s') for specified index ('%s') - valid types: %s",
					category, index, searchFieldsMap.get(index).keySet()));
		}
	}

	private static QueryBuilder constructQuery(final Index index,
			final String search, final String category) {
		QueryBuilder query = null;
		if (category.equals("author")) { /* TODO: replace with polymorphic dispatch */
			query = processAuthor(search);
		} else if (category.equals("id")) {
			query = processId(search);
		} else if (category.equals("keyword")) {
			query = processKeyword(search);
		}
		if (index.equals(Index.GND)) {
			query = filterUndifferentiatedPersons(query);
		}
		LOG.debug("Using query: " + query);
		return query;
	}

	private static QueryBuilder processAuthor(final String search) {
		QueryBuilder query;
		final String lifeDates = "\\((\\d+)-(\\d*)\\)";
		final Matcher lifeDatesMatcher =
				Pattern.compile("[^(]+" + lifeDates).matcher(search);
		if (lifeDatesMatcher.find()) {
			query = createAuthorQuery(lifeDates, search, lifeDatesMatcher);
		} else if (search.matches("\\d+")) {
			query = matchQuery(searchFields.get(3) + ".@id", search);
		} else {
			query = nameMatchQuery(search);
		}
		return query;
	}

	private static QueryBuilder processId(final String search) {
		QueryBuilder query;
		final String fixedQuery = search.matches("ht[\\d]{9}") ?
		/* HT number -> URL (temp. until we have an HBZ-ID field) */
		"http://lobid.org/resource/" + search : search;
		query = multiMatchQuery(fixedQuery, searchFields.toArray(new String[] {}));
		return query;
	}

	private static NestedQueryBuilder processKeyword(final String search) {
		return nestedQuery(
				searchFields.get(0),
				matchQuery(searchFields.get(0) + ".@id", "http://d-nb.info/gnd/"
						+ search));
	}

	private static QueryBuilder filterUndifferentiatedPersons(QueryBuilder query) {
		/* TODO: set up a filters map if we have any more such cases */
		return boolQuery().must(query).must(
				matchQuery("@type",
						"http://d-nb.info/standards/elementset/gnd#DifferentiatedPerson")
						.operator(Operator.AND));
	}

	private static QueryBuilder createAuthorQuery(final String lifeDates,
			final String search, final Matcher matcher) {
		/* Search name in name field and birth in birth field: */
		final BoolQueryBuilder birthQuery =
				boolQuery().must(
						nameMatchQuery(search.replaceAll(lifeDates, "").trim())).must(
						matchQuery(searchFields.get(1), matcher.group(1)));
		return matcher.group(2).equals("") ? birthQuery :
		/* If we have one, search death in death field: */
		birthQuery.must(matchQuery(searchFields.get(2), matcher.group(2)));
	}

	private static QueryBuilder nameMatchQuery(final String search) {
		final MultiMatchQueryBuilder query =
				multiMatchQuery(search, searchFields.get(0)).operator(Operator.AND);
		return searchFields.size() > 3 ? query.field(searchFields.get(3)) : query;
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
