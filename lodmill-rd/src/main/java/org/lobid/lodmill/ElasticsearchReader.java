package org.lobid.lodmill;

import java.io.Reader;
import java.io.StringReader;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import org.culturegraph.mf.exceptions.MetafactureException;
import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.stream.source.Opener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads an elasticsearch index and emits the _source field of all documents .
 * 
 * @author Pascal Christoph (dr0i)
 */
@In(Void.class)
@Out(Reader.class)
@Description("Reads an elasticsearch index and emits the _source field of all documents.")
public final class ElasticsearchReader extends
		DefaultObjectPipe<String, ObjectReceiver<Reader>> implements Opener {
	private static final Logger LOG = LoggerFactory
			.getLogger(ElasticsearchReader.class);
	private String hostname;
	private String clustername;
	private String indexname;
	private int hits = 100;
	private TransportClient transportClient;

	/**
	 * Sets the elasticsearch hostname
	 * 
	 * @param hostname may be an IP or a domain name
	 */
	public void setHostname(final String hostname) {
		this.hostname = hostname;
	}

	/**
	 * Sets the elasticsearch cluster name.
	 * 
	 * @param clustername the name of the cluster
	 */
	public void setClustername(final String clustername) {
		this.clustername = clustername;
	}

	/**
	 * Sets the elasticsearch index name.
	 * 
	 * @param indexname the name of the index
	 */
	public void setIndexname(final String indexname) {
		this.indexname = indexname;
	}

	/**
	 * Sets the size of the result set .
	 * 
	 * @param size the size of the result set fetched at once
	 */
	public void setIndexname(final int size) {
		this.hits = size;
	}

	@Override
	public void process(String ignore) {
		if (hostname == null || clustername == null || indexname == null) {
			LOG.error("Pass 3 params: <hostname> <clustername> <indexname>");
			return;
		}
		initClient();
		QueryBuilder qb = QueryBuilders.matchAllQuery();
		TimeValue timeValue = TimeValue.timeValueHours(20);
		SearchResponse scrollResp =
				transportClient.prepareSearch(indexname).setSearchType(SearchType.SCAN)
						.setScroll(timeValue).setQuery(qb).setSize(hits).execute()
						.actionGet();
		int cnt = 1;
		Calendar cal = Calendar.getInstance();
		long lastTime = cal.getTimeInMillis();
		while (true) {
			scrollResp =
					transportClient.prepareSearchScroll(scrollResp.getScrollId())
							.setScroll(timeValue).execute().actionGet();
			try {
				System.out.println("Stop getting dox at point of seconds: "
						+ getDuration(lastTime));
				System.out.println("Start metafacturing results starting at hit: "
						+ cnt);
				java.util.Iterator<SearchHit> hitIt = scrollResp.getHits().iterator();
				while (hitIt.hasNext()) {

					SearchHit hit = hitIt.next();
					getReceiver().process(
							new StringReader(hit.getSource().get("mabXml").toString()));
				}
			} catch (MetafactureException e) {
				LoggerFactory.getLogger(ElasticsearchReader.class).error(
						"Problems with elasticsearch, index '" + indexname
								+ "' at doc number '" + cnt + "'", e);
				getReceiver().closeStream();
				break;
			}
			System.out.println("Metafactured results at point of seconds: "
					+ getDuration(lastTime));
			cnt += hits;
			// Break condition: No hits are returned
			if (scrollResp.getHits().getHits().length == 0) {
				getReceiver().closeStream();
				break;
			}
		}
	}

	private static int getDuration(long lastTime) {
		Calendar cal = Calendar.getInstance();
		return (int) ((cal.getTimeInMillis() - lastTime) / 1000);
	}

	private void initClient() {
		transportClient =
				new TransportClient(ImmutableSettings.settingsBuilder()
						.put("cluster.name", clustername)
						.put("client.transport.sniff", false)
						.put("client.transport.ping_timeout", 20, TimeUnit.SECONDS).build());
		transportClient.addTransportAddress(new InetSocketTransportAddress(
				hostname, 9300));

	}
}