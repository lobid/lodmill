/* Copyright 2015  hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Test;

/**
 * Uses permanent elasticsearch instance as sink.
 * 
 * @author Pascal Christoph (dr0i)
 * 
 */
@SuppressWarnings("javadoc")
public final class MabXml2ElasticsearchLobidTestOnline {

	private static TransportClient transportClient;

	@SuppressWarnings("static-method")
	@Test
	public void testOnline() {
		transportClient = new TransportClient(
				ImmutableSettings.settingsBuilder().put("cluster.name", "weywot"));
		MabXml2ElasticsearchLobidTest
				.buildAndExecuteFlow(transportClient.addTransportAddress(
						new InetSocketTransportAddress("weywot2.hbz-nrw.de", 9300)));
	}
}
