/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package models;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.culturegraph.semanticweb.sink.AbstractModelWriter.Format;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Required;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.json.simple.JSONValue;
import org.lobid.lodmill.JsonLdConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.hp.hpl.jena.shared.BadURIException;

/**
 * Documents returned from the ElasticSearch index.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class Document {

	public static final InetSocketTransportAddress ES_SERVER =
			new InetSocketTransportAddress("10.1.2.111", 9300); // NOPMD
	public static final String ES_CLUSTER_NAME = "es-lod-hydra";
	@SuppressWarnings("serial")
	public static Map<String, List<String>> searchFieldsMap =
			new HashMap<String, List<String>>() {
				{ // NOPMD
					put("lobid-index",
							Arrays.asList(
									"http://purl.org/dc/elements/1.1/creator#preferredNameForThePerson",
									"http://purl.org/dc/elements/1.1/creator#dateOfBirth",
									"http://purl.org/dc/elements/1.1/creator#dateOfDeath"));
					put("gnd-index",
							Arrays.asList(
									"http://d-nb.info/standards/elementset/gnd#preferredNameForThePerson",
									"http://d-nb.info/standards/elementset/gnd#dateOfBirth",
									"http://d-nb.info/standards/elementset/gnd#dateOfDeath"));
				}
			};

	public static String esIndex = "lobid-index";
	public static List<String> searchFields = searchFieldsMap.get(esIndex);

	private static final Client CLIENT = new TransportClient(ImmutableSettings
			.settingsBuilder().put("cluster.name", ES_CLUSTER_NAME).build())
			.addTransportAddress(ES_SERVER);

	@Required
	public transient String author;
	public transient String source;
	public transient String id; // NOPMD

	private static List<Document> docs = new ArrayList<Document>();
	private static final Logger LOG = LoggerFactory.getLogger(Document.class);

	public Document(final String id, final String source) { // NOPMD
		this.id = id;
		this.source = source;
	}

	public Document() {
		/* Empty constructor required by Play */
	}

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

	public static List<Document> all() {
		return docs;
	}

	public static void search(final Document document) {
		docs = search(document.author, esIndex);
	}

	public static List<Document> search(final String term, final String index) {
		searchFields = searchFieldsMap.get(index);
		final String search = term.toLowerCase();
		final SearchRequestBuilder requestBuilder =
				CLIENT.prepareSearch(index)
						.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
						.setQuery(constructQuery(search));
		final SearchResponse response =
				requestBuilder.setFrom(0).setSize(50).setExplain(true)
						.execute().actionGet();
		final SearchHits hits = response.getHits();
		return asDocuments(search, hits);
	}

	private static QueryBuilder constructQuery(final String search) {
		final String lifeDates = "\\((\\d+)-(\\d*)\\)";
		final Matcher matcher =
				Pattern.compile("[^(]+" + lifeDates).matcher(search);
		BoolQueryBuilder query = null;
		if (matcher.find()) {
			/* Search name in name field and birth in birth field: */
			final BoolQueryBuilder birthQuery =
					boolQuery().must(
							matchQuery(searchFields.get(0),
									search.replaceAll(lifeDates, "").trim())
									.operator(Operator.AND)).must(
							matchQuery(searchFields.get(1), matcher.group(1)));
			query = matcher.group(2).equals("") ? birthQuery :
			/* If we have one, search death in death field: */
			birthQuery.must(matchQuery(searchFields.get(2), matcher.group(2)));
		} else {
			/* Search all in name field: */
			query =
					boolQuery().must(
							matchQuery(searchFields.get(0), search).operator(
									Operator.AND));
		}
		return query;
	}

	private static List<Document> asDocuments(final String search,
			final SearchHits hits) {
		final List<Document> res = new ArrayList<Document>();
		for (SearchHit hit : hits) {
			final Document document =
					new Document(hit.getId(), new String(hit.source()));
			withAuthor(search, hit, document);
			res.add(document);
		}
		final Predicate<Document> predicate = new Predicate<Document>() {
			public boolean apply(final Document doc) {
				return doc.author != null;
			}
		};
		return ImmutableList.copyOf(Iterables.filter(res, predicate));
	}

	private static void withAuthor(final String search, final SearchHit hit,
			final Document document) {
		final Object author = hit.getSource().get(searchFields.get(0));
		if (author instanceof List
				&& ((List<?>) author).get(0) instanceof String) {
			@SuppressWarnings("unchecked")
			final List<String> list = (List<String>) author;
			document.author = firstMatchingAuthor(search, list);
		} else {
			final Object birth = hit.getSource().get(searchFields.get(1));
			final Object death = hit.getSource().get(searchFields.get(2));
			if (birth == null) {
				document.author = author.toString();
			} else {
				final String format =
						String.format("%s (%s-%s)", author.toString(),
								birth.toString(),
								death == null ? "" : death.toString());
				document.author = format;
			}
		}
	}

	private static String firstMatchingAuthor(final String search,
			final List<String> list) {
		final Predicate<String> predicate = new Predicate<String>() {
			public boolean apply(final String string) {
				return string.toLowerCase().contains(search);
			}
		};
		return Iterables.tryFind(list, predicate).orNull();
	}
}
