/* Copyright 2013  hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.antlr.runtime.RecognitionException;
import org.apache.commons.io.FileUtils;
import org.culturegraph.mf.Flux;
import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.source.FileOpener;
import org.junit.Test;

/**
 * Extracts records from xml file and writes single xml files as well as
 * transformated ntriples into filesystem.
 * 
 * @author Pascal Christoph (dr0i)
 * 
 */
@SuppressWarnings("javadoc")
public class LobidOrganisationsUpdateTest {
	String PATH = "tmp";
	String PATH_QR = "media";

	@Test
	public void testPicaXmlSplits() throws URISyntaxException, IOException {
		final FileOpener opener = new FileOpener();
		final XmlDecoder xmlDecoder = new XmlDecoder();
		XmlTee tee = new XmlTee();
		final XmlEntitySplitter xmlSplitter = new XmlEntitySplitter();
		xmlSplitter.setEntityName("metadata");
		XmlFilenameWriter xmlFilenameWriter = createXmlFilenameWriter(PATH);
		xmlSplitter.setReceiver(xmlFilenameWriter);
		tee.addReceiver(xmlSplitter);
		final PicaXmlHandler handler = new PicaXmlHandler();
		final Metamorph metamorph =
				new Metamorph("morph_zdb-isil-file-pica2ld.xml");
		final PipeLobidOrganisationEnrichment enrich = createEnricher();
		final Triples2RdfModel triple2model = new Triples2RdfModel();
		triple2model.setInput("TURTLE");
		final RdfModelFileWriter writer = createWriter(PATH);
		handler.setReceiver(metamorph).setReceiver(enrich)
				.setReceiver(triple2model).setReceiver(writer);
		tee.addReceiver(handler);
		opener.setReceiver(xmlDecoder).setReceiver(tee);
		File infile =
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("Bibdat1303pp_sample1.xml").toURI());
		opener.process(infile.getAbsolutePath());
		opener.closeStream();
		assertEquals(
				Long.parseLong("1843551003"),
				FileUtils.checksumCRC32(new File(PATH + File.separator + "DE"
						+ File.separator + "DE-Tir1.xml")));
		deleteTestFiles();
	}

	private static XmlFilenameWriter createXmlFilenameWriter(String PATH) {
		XmlFilenameWriter xmlFilenameWriter = new XmlFilenameWriter();
		xmlFilenameWriter.setStartIndex(0);
		xmlFilenameWriter.setEndIndex(2);
		xmlFilenameWriter.setTarget(PATH);
		xmlFilenameWriter
				.setProperty("/harvest/metadata/*[local-name() = 'record']/*[local-name() = 'global']/*[local-name() = 'tag'][@id='008H']/*[local-name() = 'subf'][@id='e']");
		return xmlFilenameWriter;
	}

	private static PipeLobidOrganisationEnrichment createEnricher() {
		final PipeLobidOrganisationEnrichment enrich =
				new PipeLobidOrganisationEnrichment();
		enrich.setSerialization("TURTLE");
		enrich.setGeonameFilename("geonames_DE_sample.csv");
		return enrich;
	}

	private static RdfModelFileWriter createWriter(final String PATH) {
		final RdfModelFileWriter writer = new RdfModelFileWriter();
		writer.setProperty("http://purl.org/lobid/lv#isil");
		writer.setEndIndex(2);
		writer.setStartIndex(0);
		writer.setSerialization("NTRIPLES");
		writer.setFileSuffix("nt");
		writer.setTarget(PATH);
		return writer;
	}

	@Test
	public void testFlux() throws URISyntaxException, IOException,
			RecognitionException {
		File fluxFile =
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("zdb2lobidOrganisations.flux").toURI());
		Flux.main(new String[] { fluxFile.getAbsolutePath() });
		deleteTestFiles();
	}

	private void deleteTestFiles() throws IOException {
		FileUtils.deleteDirectory(new File(PATH));
		FileUtils.deleteDirectory(new File(PATH_QR));
	}
}
