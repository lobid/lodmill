/* Copyright 2014  hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.culturegraph.mf.Flux;
import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.LiteralExtractor;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.pipe.StreamTee;
import org.culturegraph.mf.stream.source.FileOpener;
import org.culturegraph.mf.stream.source.HttpOpener;
import org.junit.Test;

import com.jcabi.aspects.Timeable;

/**
 * @author Pascal Christoph (dr0i)
 * 
 */
@SuppressWarnings("javadoc")
public final class NwbibMabXmlTar2lobidIntegrationTest {
	private static final String TARGET_PATH = "tmp";
	private static final String TEST_FILENAME = "hbz01.nt";

	@SuppressWarnings("static-method")
	@Test
	@Timeable(limit = 2000, unit = TimeUnit.SECONDS)
	public void testFlow() throws IOException, URISyntaxException {
		// hbz catalog transformation
		final FileOpener opener = new FileOpener();
		opener.setCompression("BZIP2");
		TarReader tarReader = new TarReader();
		final XmlDecoder xmlDecoder = new XmlDecoder();
		XmlTee xmlTee = new XmlTee();
		final MabXmlHandler handler = new MabXmlHandler();
		final Metamorph morph =
				new Metamorph("src/test/resources/morph-hbz01-to-lobid.xml");
		final Triples2RdfModel triple2model = new Triples2RdfModel();
		triple2model.setInput("N-TRIPLE");
		RdfModelMysqlWriter modelWriter = createModelWriter();
		modelWriter.setProperty("http://purl.org/lobid/lv#hbzID");
		triple2model.setReceiver(modelWriter);
		// tee.addReceiver(triple2model);
		StreamTee streamTee = new StreamTee();
		final Stats stats = new Stats();
		stats.setFilename("tmp.stats.csv");
		streamTee.addReceiver(stats);

		/*
		 * geo enrichment
		 */
		// OSM API lookup
		// make OSM API URL and lookup that
		StreamTee streamTeeGeo = new StreamTee();
		streamTee.addReceiver(streamTeeGeo);
		PipeEncodeTriples encoder = new PipeEncodeTriples();
		streamTeeGeo.addReceiver(encoder);
		encoder.setReceiver(triple2model);
		final Metamorph morphCreateOsmURl =
				new Metamorph("src/test/resources/morph-nwbibhbz01-buildGeoOsmUrl.xml");
		streamTeeGeo.addReceiver(morphCreateOsmURl);
		// lookup and parse OSM API URL
		final LiteralExtractor literalExtractor = new LiteralExtractor();
		morphCreateOsmURl.setReceiver(literalExtractor);
		final HttpOpener httpOpener = new HttpOpener();
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
		MysqlWriter sqlWriterOsmUrl = new MysqlWriter();
		sqlWriterOsmUrl.setDbname("lobid");
		sqlWriterOsmUrl.setDbProtocolAndAdress("jdbc:mysql://localhost:33061/");
		sqlWriterOsmUrl.setPassword("tzSblDEUGC1XhJB7");
		sqlWriterOsmUrl.setTablename("NrwPlacesOsmUrl");
		sqlWriterOsmUrl.setUsername("debian-sys-maint");
		morphCreateOsmMysqlRow.setReceiver(sqlWriterOsmUrl);
		// streamTeeOsmUrl.addReceiver(sqlWriterOsmUrl);

		// Geonames API lookup
		// lookup geonames with generated URL
		final LiteralExtractor literalExtractorGeonames = new LiteralExtractor();
		final HttpOpener geonamesHttpOpener = new HttpOpener();
		final JsonDecoder jsonGeonamesDecoder = new JsonDecoder();
		morphOSM.setReceiver(literalExtractorGeonames);
		literalExtractorGeonames.setReceiver(geonamesHttpOpener);
		geonamesHttpOpener.setReceiver(jsonGeonamesDecoder);
		final Metamorph morphGeonames =
				new Metamorph(Thread.currentThread().getContextClassLoader()
						.getResource("morph-jsonGeonames2mysqlRow.xml").getFile());
		jsonGeonamesDecoder.setReceiver(morphGeonames);

		// writing into SQL DBMS
		MysqlWriter sqlWriter = new MysqlWriter();
		sqlWriter.setDbname("lobid");
		sqlWriter.setDbProtocolAndAdress("jdbc:mysql://localhost:33061/");
		sqlWriter.setPassword("tzSblDEUGC1XhJB7");
		sqlWriter.setTablename("NrwPlacesGeonamesId");
		sqlWriter.setUsername("debian-sys-maint");

		morphGeonames.setReceiver(sqlWriter);

		XmlEntitySplitter xmlEntitySplitter = new XmlEntitySplitter();
		xmlEntitySplitter.setEntityName("ListRecords");
		XmlFilenameWriter xmlFilenameWriter = createXmlFilenameWriter();
		xmlTee.setReceiver(handler).setReceiver(morph).setReceiver(streamTee);
		xmlTee.addReceiver(xmlEntitySplitter);
		xmlEntitySplitter.setReceiver(xmlFilenameWriter);
		opener.setReceiver(tarReader).setReceiver(xmlDecoder).setReceiver(xmlTee);

		opener.process(new File("src/test/resources/hbz01XmlClobs.tar.bz2")
				.getAbsolutePath());

		opener.closeStream();

		// final File testFile =
		// AbstractIngestTests.concatenateGeneratedFilesIntoOneFile(TARGET_PATH
		// + "/" + TARGET_SUBPATH, TEST_FILENAME);
		final File testFile = new File(TEST_FILENAME);
		StringBuilder sb = new StringBuilder();
		try {
			// the "REPLACE" is no standard ANSI SQL, only works with MySQL
			PreparedStatement ps =
					modelWriter.conn.prepareStatement("SELECT * FROM resources ");
			ResultSet res = ps.executeQuery();

			while (res.next()) {
				sb.append(res.getString(2));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		FileUtils.writeStringToFile(testFile, sb.toString(), false);
		// // positive test
		AbstractIngestTests.compareFilesDefaultingBNodes(testFile, new File(Thread
				.currentThread().getContextClassLoader().getResource(TEST_FILENAME)
				.toURI()));
		// // negative test
		AbstractIngestTests.checkIfNoIntersection(
				testFile,
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("hbz01negatives.ttl").toURI()));
		testFile.deleteOnExit();
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
		modelWriter.setDbname("lobid");
		modelWriter.setTablename("resources");
		modelWriter.setUsername("debian-sys-maint");
		modelWriter.setPassword("tzSblDEUGC1XhJB7");
		modelWriter.setDbProtocolAndAdress("jdbc:mysql://localhost:3306/");
		return modelWriter;
	}

	@SuppressWarnings("static-method")
	// @Test
	public void testFlux() throws URISyntaxException {
		File fluxFile =
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("hbz01-to-lobid.flux").toURI());
		try {
			Flux.main(new String[] { fluxFile.getAbsolutePath() });
		} catch (Exception e) {
			System.err.println(e);
		}
	}
}
