/* Copyright 2013  Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.pipe.ObjectTee;
import org.culturegraph.mf.stream.reader.XmlReaderBase;
import org.culturegraph.mf.stream.sink.ObjectStdoutWriter;
import org.culturegraph.mf.stream.source.FileOpener;
import org.junit.Test;

/**
 * @author Pascal Christoph
 * 
 */
@SuppressWarnings("javadoc")
public final class MabXml2lobidTest extends AbstractIngestTests {

	public MabXml2lobidTest() {
		super("src/test/resources/mab2example.xml.bz2", "morph-hbz01-to-lobid.xml",
				"default_morph-stats.xml", new MabXmlReader());
	}

	@Test
	public void testStatistics() throws IOException { // NOPMD
		super.stats("mapping.textile");
	}

	@SuppressWarnings("static-method")
	@Test
	public void testFlux() throws URISyntaxException {
		final FileOpener opener = new FileOpener();
		opener.setCompression("BZIP2");
		final XmlDecoder xmlDecoder = new XmlDecoder();
		final MabXmlHandler handler = new MabXmlHandler();
		final Metamorph morph = new Metamorph("morph-hbz01-to-lobid.xml");
		PipeEncodeTriples encoder = new PipeEncodeTriples();
		final Triples2RdfModel triple2model = new Triples2RdfModel();
		triple2model.setInput("N-TRIPLE");
		final ObjectTee<String> tee = new ObjectTee<String>();
		final Triples2RdfModel triples2rdfmodel = new Triples2RdfModel();
		triples2rdfmodel.setInput("N-TRIPLE");
		RdfModelFileWriter modelWriter = new RdfModelFileWriter();
		modelWriter.setProperty("http://lobid.org/vocab/lobid#hbzID");
		modelWriter.setSerialization("N-TRIPLES");
		modelWriter.setStartIndex(2);
		modelWriter.setEndIndex(5);
		String targetDirectory = "tmp";
		modelWriter.setTarget(targetDirectory);
		triple2model.setReceiver(modelWriter);
		final ObjectStdoutWriter<String> writer = new ObjectStdoutWriter<String>();
		tee.addReceiver(writer);
		tee.addReceiver(triple2model);
		opener.setReceiver(xmlDecoder).setReceiver(handler).setReceiver(morph)
				.setReceiver(encoder).setReceiver(tee);// )setReceiver(writer);
		File infile =
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("mab2example.xml.bz2").toURI());
		opener.process(infile.getAbsolutePath());
		opener.closeStream();
		AbstractIngestTests.compareFiles(
				new File(targetDirectory + "/002/002.nt"),
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("hbz01-to-lobid-output_test.nt").toURI()));
	}

	private static class MabXmlReader extends XmlReaderBase {
		// Create a reader for mab XML.
		public MabXmlReader() {
			super(new MabXmlHandler());
		}
	}

}
