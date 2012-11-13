/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package models;

import java.util.ArrayList;
import java.util.List;

import org.culturegraph.semanticweb.sink.AbstractModelWriter.Format;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Required;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.json.simple.JSONValue;
import org.lobid.lodmill.JsonLdConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
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
	public static final String ES_INDEX = "json-ld-index";
	public static final String SEARCH_FIELD =
			"http://purl.org/dc/elements/1.1/creator#preferredNameForThePerson";

	@Required
	public transient String author;
	public transient String source;
	public transient String id;// NOPMD

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
		docs = search(document.author);
	}

	public static List<Document> search(final String term) {
		final String search = term.toLowerCase(); // NOPMD
		final Client client =
				new TransportClient(ImmutableSettings.settingsBuilder()
						.put("cluster.name", ES_CLUSTER_NAME).build())
						.addTransportAddress(ES_SERVER);
		final SearchResponse response =
				client.prepareSearch(ES_INDEX)
						.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
						.setQuery(QueryBuilders.termQuery(SEARCH_FIELD, search))
						.setFrom(0).setSize(10).setExplain(true).execute()
						.actionGet();
		final SearchHits hits = response.getHits();
		return asDocuments(search, hits);
	}

	private static List<Document> asDocuments(final String search,
			final SearchHits hits) {
		final List<Document> res = new ArrayList<Document>();
		for (SearchHit hit : hits) {
			final Document document =
					new Document(hit.getId(), new String(hit.source())); // NOPMD
			withAuthor(search, hit, document);
			res.add(document);
		}
		return res;
	}

	private static void withAuthor(final String search, final SearchHit hit,
			final Document document) {
		final Object author = hit.getSource().get(SEARCH_FIELD);
		if (author instanceof List
				&& ((List<?>) author).get(0) instanceof String) {
			@SuppressWarnings("unchecked")
			final List<String> list = (List<String>) author;
			final Predicate<String> predicate = new Predicate<String>() { // NOPMD
						public boolean apply(final String string) {
							return string.toLowerCase().contains(search); // NOPMD
						}
					};
			document.author = Iterables.find(list, predicate);
		} else {
			document.author = author.toString();
		}
	}
}
