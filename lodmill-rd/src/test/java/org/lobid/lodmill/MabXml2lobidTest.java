/* Copyright 2013  Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.pipe.ObjectTee;
import org.culturegraph.mf.stream.pipe.StreamTee;
import org.culturegraph.mf.stream.reader.XmlReaderBase;
import org.culturegraph.mf.stream.sink.ObjectStdoutWriter;
import org.culturegraph.mf.stream.source.FileOpener;
import org.junit.Test;

/**
 * @author Pascal Christoph
 * 
 */
@SuppressWarnings("javadoc")
public final class MabXml2lobidTest {

	@SuppressWarnings("static-method")
	@Test
	public void testFlow() throws URISyntaxException, IOException {
		final FileOpener opener = new FileOpener();
		opener.setCompression("BZIP2");
		final XmlDecoder xmlDecoder = new XmlDecoder();
		final MabXmlHandler handler = new MabXmlHandler();
		final Metamorph morph = new Metamorph("morph-hbz01-to-lobid.xml");
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
		StreamTee streamTee = new StreamTee();
		final Stats stats = new Stats();
		streamTee.addReceiver(stats);
		PipeEncodeTriples encoder = new PipeEncodeTriples();
		streamTee.addReceiver(encoder);
		encoder.setReceiver(tee);
		opener.setReceiver(xmlDecoder).setReceiver(handler).setReceiver(morph)
				.setReceiver(streamTee);
		File infile =
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("mab2example.xml.bz2").toURI());
		opener.process(infile.getAbsolutePath());
		Stats.writeTextileMappingTable(stats.sortedByValuesDescending(), new File(
				"/dev/null"));
		opener.closeStream();
		AbstractIngestTests.compareFiles(new File(targetDirectory
				+ "/002/HT002948556.nt"), new File(Thread.currentThread()
				.getContextClassLoader().getResource("hbz01-to-lobid-output_test.nt")
				.toURI()));
	}

	private static class MabXmlReader extends XmlReaderBase {
		// Create a reader for mab XML.
		public MabXmlReader() {
			super(new MabXmlHandler());
		}
	}

}
