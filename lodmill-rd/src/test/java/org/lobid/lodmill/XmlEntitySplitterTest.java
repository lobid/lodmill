/* Copyright 2013  Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.culturegraph.mf.Flux;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.sink.EventList;
import org.culturegraph.mf.stream.sink.StreamValidator;
import org.culturegraph.mf.stream.source.FileOpener;
import org.junit.Test;

/**
 * @author Pascal Christoph
 * 
 */
@SuppressWarnings("javadoc")
public class XmlEntitySplitterTest {

	@Test
	public void testFlow() throws URISyntaxException {
		final FileOpener opener = new FileOpener();
		final XmlDecoder xmldecoder = new XmlDecoder();
		final XmlEntitySplitter xmlsplitter = new XmlEntitySplitter();
		xmlsplitter.setEntityName("Description");
		final EventList expected = new EventList();
		expected.startRecord("0");
		expected
				.literal(
						"entity",
						"<root  xmlns:rdf=\"ns#\"><rdf:Description rdf:about=\"1\"> <a rdf:resource=\"r1\">1</a></rdf:Description></root>");
		expected.endRecord();
		expected.startRecord("1");
		expected
				.literal(
						"entity",
						"<root  xmlns:rdf=\"ns#\"><rdf:Description rdf:about=\"2\"> <a rdf:resource=\"r2\">2</a></rdf:Description></root>");
		expected.endRecord();
		final StreamValidator validator = new StreamValidator(expected.getEvents());
		opener.setReceiver(xmldecoder).setReceiver(xmlsplitter)
				.setReceiver(validator);
		File infile =
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("xmlEntities.xml").toURI());
		opener.process(infile.getAbsolutePath());
		opener.closeStream();
	}

	@Test
	public void testFlux() throws IOException, URISyntaxException,
			org.antlr.runtime.RecognitionException {
		File fluxFile =
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("xmlEntitySplitter.flux").toURI());
		Flux.main(new String[] { fluxFile.getAbsolutePath() });
	}
}
