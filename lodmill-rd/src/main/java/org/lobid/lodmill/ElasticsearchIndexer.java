/* Copyright 2013-015 Fabian Steeg, Pascal Christoph, hbz. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.io.CharStreams;

/**
 * Index JSON into elasticsearch.
 * 
 * @author Pascal Christoph (dr0i)
 * @author Fabian Steeg (fsteeg)
 */
@In(HashMap.class)
@Out(Void.class)
public class ElasticsearchIndexer extends
		DefaultObjectPipe<HashMap<String, String>, ObjectReceiver<Void>> {

	private static final Logger LOG = LoggerFactory
			.getLogger(ElasticsearchIndexer.class);
	private String hostname;
	private String clustername;
	private BulkRequestBuilder bulkRequest;
	private Builder CLIENT_SETTINGS;
	private InetSocketTransportAddress NODE;
	private TransportClient tc;
	private UpdateRequest updateRequest;
	private Client client;
	private int retries = 40;
	// collect so many documents before bulk indexing them all
	private int bulkSize = 5000;
	private int docs = 0;
	private String indexName;
	private boolean updateIndex;
	private String aliasSuffix;

	/**
	 * Keys to get index properties and the json document ("graph")
	 */
	@SuppressWarnings("javadoc")
	public static enum Properties {
		INDEX("_index"), TYPE("_type"), ID("_id"), PARENT("_parent"), GRAPH("graph");
		private final String name;

		Properties(final String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

	@Override
	protected void onCloseStream() {
		// feed the rest of the bulk
		bulkRequest.execute().actionGet();
		if (!aliasSuffix.equals("NOALIAS") && !updateIndex
				&& !aliasSuffix.toLowerCase().contains("test"))
			updateAliases(indexName, aliasSuffix);
		bulkRequest.setRefresh(true);
	}

	// TODO use BulkProcessorbuilder by updating to ES 1.5
	@Override
	public void onSetReceiver() {
		this.CLIENT_SETTINGS =
				ImmutableSettings.settingsBuilder().put("cluster.name",
						this.clustername);
		this.NODE = new InetSocketTransportAddress(this.hostname, 9300);
		this.tc =
				new TransportClient(this.CLIENT_SETTINGS
						.put("client.transport.sniff", false)
						.put("client.transport.ping_timeout", 120, TimeUnit.SECONDS)
						.build());
		this.client = this.tc.addTransportAddress(this.NODE);
		bulkRequest = new BulkRequestBuilder(client);
		bulkRequest = client.prepareBulk();
		if (updateIndex) {
			getNewestIndex();
		} else
			createIndex();
		bulkRequest.setRefresh(false);
	}

	@Override
	public void process(final HashMap<String, String> json) {
		updateRequest =
				new UpdateRequest(indexName, json.get(Properties.TYPE.getName()),
						json.get(Properties.ID.getName())).doc(json.get(Properties.GRAPH
						.getName()));
		updateRequest.docAsUpsert(true);
		if (json.containsKey(Properties.PARENT.getName())) {
			updateRequest.parent(json.get(Properties.PARENT.getName()));
		}
		bulkRequest.add(updateRequest);
		docs++;
		while (docs > bulkSize && retries > 0) {
			try {
				bulkRequest.execute().actionGet();
				docs = 0;
				bulkRequest = client.prepareBulk();
				bulkRequest.setRefresh(false);
				break; // stop retry-while
			} catch (final NoNodeAvailableException e) {
				retries--;
				try {
					Thread.sleep(10000);
				} catch (final InterruptedException x) {
					x.printStackTrace();
				}
				LOG.warn("Retry indexing record" + json.get(Properties.ID.getName())
						+ ":" + e.getMessage() + " (" + retries + " more retries)");
			}
		}
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
	 * Sets the elasticsearch hostname
	 * 
	 * @param hostname may be an IP or a domain name
	 */
	public void setHostname(final String hostname) {
		this.hostname = hostname;
	}

	/**
	 * Sets the elasticsearch index name
	 * 
	 * @param indexname name of the index
	 */
	public void setIndexName(final String indexname) {
		this.indexName = indexname;
	}

	/**
	 * Sets the suffix of elasticsearch index alias suffix
	 * 
	 * @param aliasSuffix may be an IP or a domain name
	 */
	public void setIndexAliasSuffix(String aliasSuffix) {
		this.aliasSuffix = aliasSuffix;

	}

	/**
	 * Sets the elasticsearch index name
	 * 
	 * @param updateIndex name of the index
	 */
	public void setUpdateNewestIndex(final boolean updateIndex) {
		this.updateIndex = updateIndex;
	}

	private void getNewestIndex() {
		String indexNameWithoutTimestamp = indexName.replaceAll("20.*", "");
		final SortedSetMultimap<String, String> indices = groupByIndexCollection();
		for (String prefix : indices.keySet()) {
			final SortedSet<String> indicesForPrefix = indices.get(prefix);
			final String newestIndex = indicesForPrefix.last();
			if (newestIndex.startsWith(indexNameWithoutTimestamp))
				indexName = newestIndex;
		}
		LOG.info("Going to UPDATE existing index " + indexName);
	}

	private void createIndex() {
		IndicesAdminClient adminClient = client.admin().indices();
		if (!adminClient.prepareExists(indexName).execute().actionGet().isExists()) {
			LOG.info("Going to CREATE new index " + indexName);
			adminClient.prepareCreate(indexName).setSource(config()).execute()
					.actionGet();
		}
	}

	private static String config() {
		String res = null;
		try {
			final InputStream config =
					Thread.currentThread().getContextClassLoader()
							.getResourceAsStream("index-config.json");
			try (InputStreamReader reader = new InputStreamReader(config, "UTF-8")) {
				res = CharStreams.toString(reader);
			}
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return res;
	}

	private void updateAliases(final String name, final String suffix) {
		final SortedSetMultimap<String, String> indices = groupByIndexCollection();
		for (String prefix : indices.keySet()) {
			final SortedSet<String> indicesForPrefix = indices.get(prefix);
			final String newIndex = indicesForPrefix.last();
			final String newAlias = prefix + suffix;
			LOG.info("Prefix " + prefix + ", newest index: " + newIndex);
			removeOldAliases(indicesForPrefix, newAlias);
			if (!name.equals(newAlias) && !newIndex.equals(newAlias))
				createNewAlias(newIndex, newAlias);
			deleteOldIndices(name, indicesForPrefix);
		}
	}

	private SortedSetMultimap<String, String> groupByIndexCollection() {
		final SortedSetMultimap<String, String> indices = TreeMultimap.create();
		for (String index : client.admin().indices().prepareStats().execute()
				.actionGet().getIndices().keySet()) {
			final String[] nameAndTimestamp = index.split("-(?=\\d)");
			indices.put(nameAndTimestamp[0], index);
		}
		return indices;
	}

	private void removeOldAliases(final SortedSet<String> indicesForPrefix,
			final String newAlias) {
		for (String name : indicesForPrefix) {
			final Set<String> aliases = aliases(name);
			for (String alias : aliases) {
				if (alias.equals(newAlias)) {
					LOG.info("Delete alias index,alias: " + name + "," + alias);
					client.admin().indices().prepareAliases().removeAlias(name, alias)
							.execute().actionGet();
				}
			}
		}
	}

	private void createNewAlias(final String newIndex, final String newAlias) {
		LOG.info("Create alias index,alias: " + newIndex + "," + newAlias);
		client.admin().indices().prepareAliases().addAlias(newIndex, newAlias)
				.execute().actionGet();
	}

	private void deleteOldIndices(final String name,
			final SortedSet<String> allIndices) {
		if (allIndices.size() >= 3) {
			final List<String> list = new ArrayList<>(allIndices);
			list.remove(name);
			for (String indexToDelete : list.subList(0, list.size() - 2)) {
				if (aliases(indexToDelete).isEmpty()) {
					LOG.info("Deleting index: " + indexToDelete);
					client.admin().indices()
							.delete(new DeleteIndexRequest(indexToDelete)).actionGet();
				}
			}
		}
	}

	private Set<String> aliases(final String name) {
		final ClusterStateRequest clusterStateRequest =
				Requests.clusterStateRequest().nodes(true).indices(name);
		return Sets.newHashSet(client.admin().cluster().state(clusterStateRequest)
				.actionGet().getState().getMetaData().aliases().keysIt());
	}

}
