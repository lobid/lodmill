/* Copyright 2015  hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.sink.ObjectFileWriter;
import org.culturegraph.mf.stream.source.FileOpener;
import org.junit.Test;

/**
 * Transform hbz01 Aleph Mab XML catalog data into lobid elasticsearch ready
 * JSON-LD.
 * 
 * @author Pascal Christoph (dr0i)
 * 
 */
@SuppressWarnings("javadoc")
public final class MabXml2lobidJsonTest {

	private static final String TEST_FILENAME = "hbz01.json";

	@SuppressWarnings("static-method")
	@Test
	public void testFlow() throws URISyntaxException {
		buildAndExecuteFlow();
		File testFile = new File(TEST_FILENAME);
		AbstractIngestTests.compareFilesDefaultingBNodes(testFile, new File(Thread
				.currentThread().getContextClassLoader().getResource(TEST_FILENAME)
				.toURI()));
		testFile.deleteOnExit();
	}

	private static void buildAndExecuteFlow() {
		// hbz catalog transformation
		final FileOpener opener = new FileOpener();
		opener.setCompression("BZIP2");
		final Triples2RdfModel triple2model = new Triples2RdfModel();
		triple2model.setInput("N-TRIPLE");
		// @TODO test elasticsearch indexing runner with mock up
		opener
				.setReceiver(new TarReader())
				.setReceiver(new XmlDecoder())
				.setReceiver(new MabXmlHandler())
				.setReceiver(
						new Metamorph("src/main/resources/morph-hbz01-to-lobid.xml"))
				.setReceiver(new PipeEncodeTriples())
				.setReceiver(triple2model)
				.setReceiver(new RdfModel2ElasticsearchJsonLd())
				.setReceiver(
						new ObjectFileWriter<HashMap<String, String>>(TEST_FILENAME));
		opener.process(new File("src/test/resources/hbz01XmlClobs.tar.bz2")
				.getAbsolutePath());
		opener.closeStream();

	}
}
