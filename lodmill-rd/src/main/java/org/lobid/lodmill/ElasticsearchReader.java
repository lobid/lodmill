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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
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
public final class ElasticsearchReader
		extends DefaultObjectPipe<String, ObjectReceiver<Reader>>implements Opener {
	private static final Logger LOG =
			LoggerFactory.getLogger(ElasticsearchReader.class);
	private String hostname;
	private String clustername;
	private String indexname;
	private int batchSize = 10;
	private int to = Integer.MAX_VALUE;
	private TransportClient transportClient;
	private SearchResponse response;
	private final long lastTime = Calendar.getInstance().getTimeInMillis();
	private String shards;

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
	 * Sets the size of the result set . Will be multiplicated with number of
	 * shards.
	 * 
	 * @param batchSize the size of the result set fetched at once
	 */
	public void setBatchSize(final int batchSize) {
		this.batchSize = batchSize;
	}

	/**
	 * Sets which shards should be searched May be comma separated list .
	 * 
	 * @param shards the beginning of the range of the result set
	 */
	public void setShards(final String shards) {
		this.shards = shards;
	}

	/**
	 * Sets end of range of the result set .
	 * 
	 * @param to the end of the range of the result set
	 */
	public void setTo(final int to) {
		this.to = to;
	}

	@Override
	public void process(String ignore) {
		if (hostname == null || clustername == null || indexname == null) {
			LOG.error("Pass 3 params: <hostname> <clustername> <indexname>");
			return;
		}
		initScrollSearch();
		logStatus();
		harvestAndProcess();
	}

	private void initScrollSearch() {
		transportClient = new TransportClient(ImmutableSettings.settingsBuilder()
				.put("cluster.name", clustername).put("client.transport.sniff", false)
				.put("client.transport.ping_timeout", 20, TimeUnit.SECONDS).build());
		transportClient
				.addTransportAddress(new InetSocketTransportAddress(hostname, 9300));
		response = transportClient.prepareSearch(indexname)
				.setSearchType(SearchType.SCAN).setPreference("_shards:" + shards)
				.setScroll(TimeValue.timeValueHours(20))
				.setQuery(QueryBuilders.matchAllQuery()).setExplain(false)
				.setSize(batchSize).execute().actionGet();
	}

	private void logStatus() {
		LOG.info("Amount of shards: " + transportClient.prepareSearch(indexname)
				.execute().actionGet().getTotalShards());
		LOG.info("Starting querying in partitions of "
				+ (batchSize * response.getTotalShards()));
	}

	private void harvestAndProcess() throws ElasticsearchException {
		int cnt = 0;
		while (true) {
			try {
				response = transportClient.prepareSearchScroll(response.getScrollId())
						.setScroll("1h").execute().actionGet();
				java.util.Iterator<SearchHit> hitIt = response.getHits().iterator();
				while (hitIt.hasNext()) {
					getReceiver().process(new StringReader(
							hitIt.next().getSource().get("mabXml").toString()));
					cnt++;
				}
			} catch (MetafactureException e) {
				LOG.error("Problems with elasticsearch, index '" + indexname
						+ "' at doc number '" + cnt + "'", e);
				getReceiver().closeStream();
				break;
			}
			LOG.info("Doc " + cnt + " ,sec:"
					+ ((Calendar.getInstance().getTimeInMillis() - lastTime) / 1000));
			// Break condition: No hits are returned or range is exceeded
			if (response.getHits().getHits().length == 0 || cnt >= to) {
				getReceiver().closeStream();
				break;
			}
		}
	}
}