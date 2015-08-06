/* Copyright 2014  hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;
import java.net.URISyntaxException;

import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.runner.Flux;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.pipe.StreamTee;
import org.junit.Test;

/**
 * Gets the data out of an elasticsearch index. Sink is a MySQL DB. The port is
 * deliberately hardwired to 3306. Skip this test if you have already a running
 * daemon on port 3306.
 * 
 * @author Pascal Christoph (dr0i)
 * 
 */
@SuppressWarnings("javadoc")
public final class MabXmlElasticsearch2lobidIntegrationOnlineTest {
	private static final String TARGET_PATH = "tmp";
	private static final String DB_PROTOCOL_AND_ADDRESS =
			"jdbc:mysql://localhost:3306/";
	private static final String DB_PASSWORD = "tzSblDEUGC1XhJB7";
	private static final String DB_DBNAME = "lobid";

	@SuppressWarnings("static-method")
	@Test
	public void testFlow() {
		// hbz catalog transformation
		final ElasticsearchReader opener = new ElasticsearchReader();
		opener.setClustername("quaoar");
		opener.setHostname("193.30.112.171");
		opener.setIndexname("hbz01");
		opener.setShards("0,1,2,3,4");

		final XmlDecoder xmlDecoder = new XmlDecoder();
		XmlTee xmlTee = new XmlTee();
		final MabXmlHandler handler = new MabXmlHandler();
		final Metamorph morph =
				new Metamorph("src/main/resources/morph-hbz01-to-lobid.xml");
		final Triples2RdfModel triple2model = new Triples2RdfModel();
		triple2model.setInput("N-TRIPLE");
		RdfModelMysqlWriter modelWriter = createModelWriter();
		triple2model.setReceiver(modelWriter);
		StreamTee streamTee = new StreamTee();
		final Stats stats = new Stats();
		stats.setFilename("tmp.stats.csv");
		streamTee.addReceiver(stats);
		StreamTee streamTeeGeo = new StreamTee();
		streamTee.addReceiver(streamTeeGeo);
		PipeEncodeTriples encoder = new PipeEncodeTriples();
		streamTeeGeo.addReceiver(encoder);
		encoder.setReceiver(triple2model);
		XmlEntitySplitter xmlEntitySplitter = new XmlEntitySplitter();
		xmlEntitySplitter.setEntityName("ListRecords");
		XmlFilenameWriter xmlFilenameWriter = createXmlFilenameWriter();
		xmlTee.setReceiver(handler).setReceiver(morph).setReceiver(streamTee);
		xmlTee.addReceiver(xmlEntitySplitter);
		xmlEntitySplitter.setReceiver(xmlFilenameWriter);
		// StreamToReader streamToString = new StreamToReader();
		opener.setReceiver(xmlDecoder).setReceiver(xmlTee);
		opener.process("");
		opener.closeStream();
	}

	private static XmlFilenameWriter createXmlFilenameWriter() {
		XmlFilenameWriter xmlFilenameWriter = new XmlFilenameWriter();
		xmlFilenameWriter.setStartIndex(2);
		xmlFilenameWriter.setEndIndex(7);
		xmlFilenameWriter.setTarget(TARGET_PATH + "/xml");
		xmlFilenameWriter.setProperty(
				"/OAI-PMH/ListRecords/record/metadata/record/datafield[@tag='001']/subfield[@code='a']");
		xmlFilenameWriter.setCompression("bz2");
		xmlFilenameWriter.setFileSuffix("");
		xmlFilenameWriter.setEncoding("utf8");
		return xmlFilenameWriter;
	}

	private static RdfModelMysqlWriter createModelWriter() {
		RdfModelMysqlWriter modelWriter = new RdfModelMysqlWriter();
		modelWriter.setProperty("http://purl.org/lobid/lv#hbzID");
		modelWriter.setDbname(DB_DBNAME);
		modelWriter.setTablename("resources");
		modelWriter.setUsername("debian-sys-maint");
		modelWriter.setPassword(DB_PASSWORD);
		modelWriter.setDbProtocolAndAdress(DB_PROTOCOL_AND_ADDRESS);
		return modelWriter;
	}

	@SuppressWarnings("static-method")
	@Test
	public void testFlux() throws URISyntaxException {
		File fluxFile = new File(Thread.currentThread().getContextClassLoader()
				.getResource("hbz01ES-to-lobid.flux").toURI());
		try {
			Flux.main(new String[] { fluxFile.getAbsolutePath() });
		} catch (Exception e) {
			System.err.println(e);
		}
	}
}
