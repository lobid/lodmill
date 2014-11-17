/* Copyright 2014  hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;
import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.runner.Flux;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.pipe.StreamTee;
import org.culturegraph.mf.stream.source.FileOpener;
import org.junit.Test;

/**
 * Transform hbz01 MAB2 catalog data. Using wikidata concordance table (which
 * was build with {@link WikidataGeoJson2Mysql}).The port is deliberately
 * hardwired to 3306. Skip this test if you have already a running daemon on
 * port 3306.
 * 
 * @author Pascal Christoph (dr0i)
 * 
 */
@SuppressWarnings("javadoc")
public final class MabXmlWikidata2lobidIntegrationTest {
	private static final String TARGET_PATH = "tmp";
	private static final String TEST_FILENAME = "hbz01.nt";
	private static final String DB_PROTOCOL_AND_ADDRESS =
			"jdbc:mysql://localhost:3306/";
	private static final String DB_PASSWORD = "tzSblDEUGC1XhJB7";
	private static final String DB_DBNAME = "lobid";
	private static PreparedStatement ps;
	private static ResultSet res;

	@SuppressWarnings("static-method")
	@Test
	public void testFlow() throws IOException, URISyntaxException {
		RdfModelMysqlWriter modelWriter = buildFlow();
		final File testFile = dumpMysqlToFile(modelWriter);
		// positive test
		AbstractIngestTests.compareFilesDefaultingBNodes(testFile, new File(Thread
				.currentThread().getContextClassLoader().getResource(TEST_FILENAME)
				.toURI()));
		// negative test
		AbstractIngestTests.checkIfNoIntersection(
				testFile,
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("hbz01negatives.ttl").toURI()));
		testFile.deleteOnExit();
	}

	private static RdfModelMysqlWriter buildFlow() {
		// hbz catalog transformation
		final FileOpener opener = new FileOpener();
		opener.setCompression("BZIP2");
		TarReader tarReader = new TarReader();
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
		xmlEntitySplitter.setTopLevelElement("OAI-PMH");
		XmlFilenameWriter xmlFilenameWriter = createXmlFilenameWriter();
		xmlTee.setReceiver(handler).setReceiver(morph).setReceiver(streamTee);
		xmlTee.addReceiver(xmlEntitySplitter);
		xmlEntitySplitter.setReceiver(xmlFilenameWriter);
		opener.setReceiver(tarReader).setReceiver(xmlDecoder).setReceiver(xmlTee);
		opener.process(new File("src/test/resources/hbz01XmlClobs.tar.bz2")
				.getAbsolutePath());
		opener.closeStream();
		return modelWriter;
	}

	private static File dumpMysqlToFile(RdfModelMysqlWriter modelWriter)
			throws IOException {
		final File testFile = new File(TEST_FILENAME);
		StringBuilder sb = new StringBuilder();
		try {
			ps = modelWriter.conn.prepareStatement("SELECT * FROM resourcesAll ");
			res = ps.executeQuery();

			while (res.next()) {
				sb.append(res.getString(2));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		FileUtils.writeStringToFile(testFile, sb.toString(), false);
		return testFile;
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
		modelWriter.setTablename("resourcesAll");
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
						.getResource("hbz01-to-lobid.flux").toURI());
		try {
			Flux.main(new String[] { fluxFile.getAbsolutePath() });
		} catch (Exception e) {
			System.err.println(e);
		}
	}
}
