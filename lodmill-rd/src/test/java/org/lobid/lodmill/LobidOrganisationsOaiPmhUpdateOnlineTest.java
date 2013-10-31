/* Copyright 2013  Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.antlr.runtime.RecognitionException;
import org.apache.commons.io.FileUtils;
import org.culturegraph.mf.Flux;
import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.junit.Test;

/**
 * @author Pascal Christoph
 * 
 */
@SuppressWarnings("javadoc")
public class LobidOrganisationsOaiPmhUpdateOnlineTest {

	@Test
	public void testFlow() throws IOException {
		final String PATH = "tmp";
		final OaiPmhOpener opener = createOpener();
		final XmlDecoder xmldecoder = new XmlDecoder();
		final PicaXmlHandler handler = new PicaXmlHandler();
		final Metamorph metamorph =
				new Metamorph("morph_zdb-isil-file-pica2ld.xml");
		final PipeLobidOrganisationEnrichment enrich = createEnricher();
		final Triples2RdfModel triple2model = new Triples2RdfModel();
		triple2model.setInput("TURTLE");
		final RdfModelFileWriter writer = createWriter(PATH);
		opener.setReceiver(xmldecoder).setReceiver(handler).setReceiver(metamorph)
				.setReceiver(enrich).setReceiver(triple2model).setReceiver(writer);
		opener.process("http://services.d-nb.de/oai/repository");
		opener.closeStream();
		FileUtils.deleteDirectory(new File(PATH));
	}

	private static PipeLobidOrganisationEnrichment createEnricher() {
		final PipeLobidOrganisationEnrichment enrich =
				new PipeLobidOrganisationEnrichment();
		enrich.setSerialization("TURTLE");
		enrich.setGeonameFilename("geonames_DE.csv");
		return enrich;
	}

	private static OaiPmhOpener createOpener() {
		final OaiPmhOpener opener = new OaiPmhOpener();
		opener.setDateFrom("2013-08-11");
		opener.setDateUntil("2013-08-12");
		opener.setMetadataPrefix("PicaPlus-xml");
		opener.setSetSpec("bib");
		return opener;
	}

	private static RdfModelFileWriter createWriter(final String PATH) {
		final RdfModelFileWriter writer = new RdfModelFileWriter();
		writer.setProperty("http://purl.org/lobid/lv#isil");
		writer.setEndIndex(2);
		writer.setStartIndex(0);
		writer.setFileSuffix("nt");
		writer.setSerialization("N-TRIPLE");
		writer.setTarget(PATH);
		return writer;
	}

	@Test
	public void testFluxOaipmh() throws IOException, URISyntaxException,
			RecognitionException {
		File fluxFile =
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("oaipmh-zdbIsil2ld.flux").toURI());
		Flux.main(new String[] { fluxFile.getAbsolutePath() });
	}
}
