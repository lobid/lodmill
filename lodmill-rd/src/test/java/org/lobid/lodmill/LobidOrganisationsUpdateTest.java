/* Copyright 2013  Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.commons.io.FileUtils;
import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.source.FileOpener;
import org.junit.Test;

/**
 * @author Pascal Christoph
 * 
 */
@SuppressWarnings("javadoc")
public class LobidOrganisationsUpdateTest {

	@Test
	public void testFlow() throws URISyntaxException, IOException {
		final String PATH = "tmp";
		final FileOpener opener = new FileOpener();
		final XmlDecoder xmldecoder = new XmlDecoder();
		final PicaXmlHandler handler = new PicaXmlHandler();
		final Metamorph metamorph =
				new Metamorph("morph_zdb-isil-file-pica2ld.xml");
		final PipeLobidOrganisationEnrichment enrich =
				new PipeLobidOrganisationEnrichment();
		enrich.setSerialization("TURTLE");
		enrich.setGeonameFilename("geonames_DE_sample.csv");
		final Triples2RdfModel triple2model = new Triples2RdfModel();
		triple2model.setSerialization("TURTLE");
		final RdfModelFileWriter writer = new RdfModelFileWriter();
		writer.setProperty("http://purl.org/dc/terms/identifier");
		writer.setEndIndex(2);
		writer.setStartIndex(0);
		writer.setFileSuffix("nt");
		writer.setTarget(PATH);
		opener.setReceiver(xmldecoder).setReceiver(handler).setReceiver(metamorph)
				.setReceiver(enrich).setReceiver(triple2model).setReceiver(writer);
		File infile =
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("Bibdat1303pp_sample1.xml").toURI());
		opener.process(infile.getAbsolutePath());
		opener.closeStream();
		FileUtils.deleteDirectory(new File(PATH));
	}
}
