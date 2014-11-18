/* Copyright 2014  hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;

/**
 * List all values of the fields defined in morph to make statistics.
 * 
 * @author Pascal Christoph (dr0i) >
 */
@SuppressWarnings("javadoc")
public final class MabXmlElasticsearch2FieldValueStatistics {

	public static void main(String... args) {
		// hbz catalog transformation
		final ElasticsearchReader opener = new ElasticsearchReader();
		opener.setClustername("lobid-hbz");
		opener.setHostname("193.30.112.172");
		opener.setIndexname("hbz01");
		opener.setShards("0,1,2,3,4");
		final XmlDecoder xmlDecoder = new XmlDecoder();
		final MabXmlHandler handler = new MabXmlHandler();
		final Metamorph morph =
				new Metamorph("src/main/resources/morph-hbz01-fieldValues-lists.xml");
		Stats logger = new Stats();
		opener.setReceiver(xmlDecoder).setReceiver(handler).setReceiver(morph)
				.setReceiver(logger);
		opener.process("");

		opener.closeStream();

	}

}
