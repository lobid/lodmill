/* Copyright 2013,2014  Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.antlr.runtime.RecognitionException;
import org.apache.commons.io.FileUtils;
import org.culturegraph.mf.runner.Flux;
import org.culturegraph.mf.stream.converter.LiteralExtractor;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.source.FileOpener;
import org.junit.Test;

/**
 * Test gnd rdf xml transformation using gnd ontology to enrich the RDF with
 * simple RDFS by applying a reasoner to inference data.
 * 
 * @author Pascal Christoph (dr0i)
 * 
 */
@SuppressWarnings("javadoc")
public class GndXmlSplitterRdfWriterTest {
	private final String PATH = "tmp";

	/**
	 * Deliberately throws some Exceptions due to a invalid XML. Tests if the
	 * transformation of the other input is doen nonetheless.
	 * 
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	@Test
	public void testFlow() throws IOException, URISyntaxException {
		final DirReader dirReader = new DirReader();
		final FileOpener opener = new FileOpener();
		final XmlDecoder xmlDecoder = new XmlDecoder();
		final XmlEntitySplitter splitXml = new XmlEntitySplitter();
		splitXml.setEntityName("Description");
		// top level element, to be valid according to rapper and riot,see
		// http://www.w3.org/TR/rdf-syntax-grammar/#section-Syntax-complete-document
		splitXml.setTopLevelElement("rdf:RDF");
		final LiteralExtractor extractLiteral = new LiteralExtractor();
		final Triples2RdfModel triple2model = new Triples2RdfModel();
		triple2model.setInput("RDF/XML");
		triple2model.setInferenceModel(Thread.currentThread()
				.getContextClassLoader().getResource("gndo.ttl").getPath());
		triple2model.setPropertyIdentifyingTheNodeForInferencing(
				"http://d-nb.info/standards/elementset/gnd#gndIdentifier");
		final RdfModelFileWriter writer = createWriter(PATH);
		dirReader.setReceiver(opener);
		opener.setReceiver(xmlDecoder).setReceiver(splitXml)
				.setReceiver(extractLiteral).setReceiver(triple2model)
				.setReceiver(writer);
		File infile = new File(Thread.currentThread().getContextClassLoader()
				.getResource("gnd").toURI());
		dirReader.process(infile.getAbsolutePath());
		assertEquals(9, (new File(PATH + "/104/")).list().length);
		final File testFile = AbstractIngestTests
				.concatenateGeneratedFilesIntoOneFile(PATH, "gndTestOutput.nt");
		AbstractIngestTests.compareFilesDefaultingBNodes(testFile,
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("gndTest.nt").toURI()));
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
	public void testFlux()
			throws IOException, URISyntaxException, RecognitionException {
		File fluxFile = new File(Thread.currentThread().getContextClassLoader()
				.getResource("xmlSplitterRdfWriter.flux").toURI());
		Flux.main(new String[] { fluxFile.getAbsolutePath() });
		FileUtils.deleteDirectory(new File(PATH));
	}
}
