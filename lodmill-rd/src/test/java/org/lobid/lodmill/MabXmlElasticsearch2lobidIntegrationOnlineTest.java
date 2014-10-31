/* Copyright 2014  hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;
import java.net.URISyntaxException;

import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.runner.Flux;
import org.culturegraph.mf.stream.converter.LiteralExtractor;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.pipe.StreamTee;
import org.culturegraph.mf.stream.source.HttpOpener;
import org.junit.Test;

/**
 * Rather complex nested flow: gets the data out of an elasticsearch sink, uses
 * two api calls for geo enrichment, caching data in a MySQL DB used by morph
 * sql lookup function. The flux is aquivalent to the flow. The port is
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
		opener.setClustername("aither");
		opener.setHostname("193.30.112.84");
		opener.setIndexname("hbz01-20140829");

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

		// OSM API lookup
		// make OSM API URL and lookup that
		final Metamorph morphCreateOsmURl =
				new Metamorph("src/main/resources/morph-nwbibhbz01-buildGeoOsmUrl.xml");
		streamTeeGeo.addReceiver(morphCreateOsmURl);
		// lookup and parse OSM API URL
		final LiteralExtractor literalExtractor = new LiteralExtractor();
		morphCreateOsmURl.setReceiver(literalExtractor);
		final HttpOpener httpOpener = getJsonHttpOpener();
		final JsonDecoder jsonOsmDecoder = new JsonDecoder();
		literalExtractor.setReceiver(httpOpener);
		httpOpener.setReceiver(jsonOsmDecoder);

		// parse OSM result, get lat lon and make URL for geonames lookup
		final Metamorph morphOSM =
				new Metamorph(Thread.currentThread().getContextClassLoader()
						.getResource("morph-osmResult-buildGeonamesLatLonUrl.xml")
						.getFile());

		StreamTee streamTeeOsmUrl = new StreamTee();
		jsonOsmDecoder.setReceiver(streamTeeOsmUrl);
		streamTeeOsmUrl.setReceiver(morphOSM);
		final Metamorph morphCreateOsmMysqlRow =
				new Metamorph("src/main/resources/morph-jsonOsm2mysqlRow.xml");
		streamTeeOsmUrl.addReceiver(morphCreateOsmMysqlRow);

		// writing OSM url into SQL DBMS
		MysqlWriter sqlWriterOsmUrl = createMysqlWriter("NrwPlacesOsmUrl");
		morphCreateOsmMysqlRow.setReceiver(sqlWriterOsmUrl);

		// Geonames API lookup
		// lookup geonames with generated URL
		final LiteralExtractor literalExtractorGeonames = new LiteralExtractor();
		final HttpOpener geonamesHttpOpener = getJsonHttpOpener();
		final JsonDecoder jsonGeonamesDecoder = new JsonDecoder();
		morphOSM.setReceiver(literalExtractorGeonames);
		literalExtractorGeonames.setReceiver(geonamesHttpOpener);
		geonamesHttpOpener.setReceiver(jsonGeonamesDecoder);
		final Metamorph morphGeonames =
				new Metamorph(Thread.currentThread().getContextClassLoader()
						.getResource("morph-jsonGeonames2mysqlRow.xml").getFile());
		jsonGeonamesDecoder.setReceiver(morphGeonames);

		// writing into SQL DBMS
		MysqlWriter sqlWriter = createMysqlWriter("NrwPlacesGeonamesId");
		morphGeonames.setReceiver(sqlWriter);
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

	private static HttpOpener getJsonHttpOpener() {
		final HttpOpener httpOpener = new HttpOpener();
		httpOpener.setAccept("application/json");
		return httpOpener;
	}

	private static MysqlWriter createMysqlWriter(String tableName) {
		MysqlWriter sqlWriterOsmUrl = new MysqlWriter();
		sqlWriterOsmUrl.setDbname(DB_DBNAME);
		sqlWriterOsmUrl.setDbProtocolAndAdress(DB_PROTOCOL_AND_ADDRESS);
		sqlWriterOsmUrl.setPassword(DB_PASSWORD);
		sqlWriterOsmUrl.setTablename(tableName);
		sqlWriterOsmUrl.setUsername("debian-sys-maint");
		return sqlWriterOsmUrl;
	}

	private static XmlFilenameWriter createXmlFilenameWriter() {
		XmlFilenameWriter xmlFilenameWriter = new XmlFilenameWriter();
		xmlFilenameWriter.setStartIndex(2);
		xmlFilenameWriter.setEndIndex(7);
		xmlFilenameWriter.setTarget(TARGET_PATH + "/xml");
		xmlFilenameWriter
				.setProperty("/OAI-PMH/ListRecords/record/metadata/record/datafield[@tag='001']/subfield[@code='a']");
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
		File fluxFile =
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("hbz01ES-to-lobid.flux").toURI());
		try {
			Flux.main(new String[] { fluxFile.getAbsolutePath() });
		} catch (Exception e) {
			System.err.println(e);
		}
	}
}
