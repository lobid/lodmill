/* Copyright 2014  hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;
import java.net.URISyntaxException;

import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.source.FileOpener;
import org.junit.Test;

/**
 * Test storing URN's as URI's.
 * 
 * @author Pascal Christoph (dr0i)
 * 
 */
@SuppressWarnings("javadoc")
public final class UrnAsUriTest {
	private static final String TARGET_PATH = "tmp";
	private static final String TEST_FILENAME = "CT003012479.nt";
	private static final String TARGET_SUBPATH = "/nt/";

	@SuppressWarnings("static-method")
	@Test
	public void testFlow() throws URISyntaxException {
		final FileOpener opener = new FileOpener();
		opener.setCompression("BZIP2");
		final XmlDecoder xmlDecoder = new XmlDecoder();
		final MabXmlHandler handler = new MabXmlHandler();
		final Metamorph morph =
				new Metamorph("src/test/resources/morph-hbz01-to-lobid.xml");
		final Triples2RdfModel triple2model = new Triples2RdfModel();
		triple2model.setInput("N-TRIPLE");
		RdfModelFileWriter modelWriter = createModelWriter();
		modelWriter.setProperty("http://purl.org/lobid/lv#hbzID");
		triple2model.setReceiver(modelWriter);
		PipeEncodeTriples encoder = new PipeEncodeTriples();
		encoder.setStoreUrnAsUri("true");
		encoder.setReceiver(triple2model);
		xmlDecoder.setReceiver(handler).setReceiver(morph);
		morph.setReceiver(encoder);
		opener.setReceiver(xmlDecoder);
		opener.process(new File("src/test/resources/CT003012479.bz2")
				.getAbsolutePath());
		opener.closeStream();
		final File testFile =
				new File(TARGET_PATH + TARGET_SUBPATH + "00301/" + TEST_FILENAME);
		AbstractIngestTests.compareFilesDefaultingBNodes(testFile, new File(Thread
				.currentThread().getContextClassLoader().getResource(TEST_FILENAME)
				.toURI()));
	}

	private static RdfModelFileWriter createModelWriter() {
		RdfModelFileWriter modelWriter = new RdfModelFileWriter();
		modelWriter.setProperty("http://lobid.org/vocab/lobid#hbzID");
		modelWriter.setSerialization("N-TRIPLES");
		modelWriter.setStartIndex(2);
		modelWriter.setEndIndex(7);
		modelWriter.setTarget(TARGET_PATH + TARGET_SUBPATH);
		return modelWriter;
	}

}
