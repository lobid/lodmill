/* Copyright 2013  Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.antlr.runtime.RecognitionException;
import org.apache.commons.io.FileUtils;
import org.culturegraph.mf.Flux;
import org.culturegraph.mf.stream.converter.LiteralExtractor;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.source.FileOpener;
import org.junit.Test;

/**
 * @author Pascal Christoph
 * 
 */
@SuppressWarnings("javadoc")
public class GndXmlSplitterRdfWriterTest {
	private final String PATH = "tmp";

	@Test
	public void testFlow() throws IOException, URISyntaxException {
		final FileOpener opener = new FileOpener();
		final XmlDecoder xmldecoder = new XmlDecoder();
		final XmlEntitySplitter splitxml = new XmlEntitySplitter();
		splitxml.setEntityName("Description");
		final LiteralExtractor extractliteral = new LiteralExtractor();
		final Triples2RdfModel triple2model = new Triples2RdfModel();
		triple2model.setInput("RDF/XML");
		final RdfModelFileWriter writer = createWriter(PATH);
		opener.setReceiver(xmldecoder).setReceiver(splitxml)
				.setReceiver(extractliteral).setReceiver(triple2model)
				.setReceiver(writer);
		File infile =
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("gndRdf.xml").toURI());
		opener.process(infile.getAbsolutePath());
		opener.closeStream();
		FileUtils.deleteDirectory(new File(PATH));
	}

	private static RdfModelFileWriter createWriter(final String PATH) {
		final RdfModelFileWriter writer = new RdfModelFileWriter();
		writer
				.setProperty("http://d-nb.info/standards/elementset/gnd#gndIdentifier");
		writer.setEndIndex(3);
		writer.setStartIndex(0);
		writer.setFileSuffix("nt");
		writer.setSerialization("N-TRIPLE");
		writer.setTarget(PATH);
		return writer;
	}

	@Test
	public void testFlux() throws IOException, URISyntaxException,
			RecognitionException {
		File fluxFile =
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("xmlSplitterRdfWriter.flux").toURI());
		Flux.main(new String[] { fluxFile.getAbsolutePath() });
		FileUtils.deleteDirectory(new File(PATH));
	}
}
