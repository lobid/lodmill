/* Copyright 2013 Jan Schnasse.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;
import java.net.URISyntaxException;

import org.culturegraph.mf.Flux;
import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.pipe.StreamLogger;
import org.culturegraph.mf.stream.pipe.StreamTee;
import org.culturegraph.mf.stream.sink.EventList;
import org.culturegraph.mf.stream.sink.ObjectStdoutWriter;
import org.culturegraph.mf.stream.sink.StreamValidator;
import org.junit.Test;

/**
 * @author Jan Schnasse
 * 
 */
public class OAIDCFlowTest {

	@SuppressWarnings("javadoc")
	@Test
	public void testDecoder() {
		final EventList expected = new EventList();
		expected.startRecord("1");
		expected.startEntity("http://lobid.org/resource/TT002234391");
		expected.literal("http://xmlns.com/foaf/0.1/isPrimaryTopicOf",
				"http://193.30.112.134/F/?func=find-c&ccl_term=IDN%3DTT002234391");
		expected.endEntity();
		expected.endRecord();

		final NTripleDecoder decoder = new NTripleDecoder();
		final StreamLogger logger = new StreamLogger("decoder");
		final StreamValidator validator = new StreamValidator(expected.getEvents());

		decoder.setReceiver(logger).setReceiver(validator);

		decoder
				.process("<http://lobid.org/resource/TT002234391> <http://xmlns.com/foaf/0.1/isPrimaryTopicOf> <http://193.30.112.134/F/?func=find-c&ccl_term=IDN%3DTT002234391> .");
		decoder.closeStream();
	}

	@SuppressWarnings("javadoc")
	@Test
	public void testFlow() {

		final HttpOpener opener = new HttpOpener();
		final StreamToStringReader reader = new StreamToStringReader();
		final NTripleDecoder decoder = new NTripleDecoder();
		final StreamLogger logger = new StreamLogger("decoder");
		final Metamorph metamorph = new Metamorph("morph-lobid-to-oaidc.xml");
		final ObjectStdoutWriter writer = new ObjectStdoutWriter();
		final StreamTee tee = new StreamTee();
		final XMLEncoder encoder = new XMLEncoder();

		metamorph.setReceiver(encoder).setReceiver(writer);
		tee.addReceiver(logger);
		tee.addReceiver(metamorph);

		opener.setReceiver(reader);
		reader.setReceiver(decoder);
		decoder.setReceiver(tee);

		opener.process("http://www.lobid.org/resource/HT015696519/about");
		opener.closeStream();

	}

	@SuppressWarnings("javadoc")
	@Test
	public void testFlux() throws URISyntaxException {

		try {
			String lobidUrl = "http://www.lobid.org/resource/HT015696519/about";
			File outfile = File.createTempFile("oaidc", "xml");
			outfile.deleteOnExit();
			File fluxFile =
					new File(Thread.currentThread().getContextClassLoader()
							.getResource("morph-lobid-to-oaidc.flux").toURI());
			Flux.main(new String[] { fluxFile.getAbsolutePath(), "url=" + lobidUrl,
					"out=" + outfile.getAbsolutePath() });
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
