/* Copyright 2013 Jan Schnasse.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.antlr.runtime.RecognitionException;
import org.culturegraph.mf.Flux;
import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.pipe.StreamLogger;
import org.culturegraph.mf.stream.pipe.StreamTee;
import org.culturegraph.mf.stream.sink.EventList;
import org.culturegraph.mf.stream.sink.ObjectStdoutWriter;
import org.culturegraph.mf.stream.sink.StreamValidator;
import org.culturegraph.mf.stream.source.HttpOpener;
import org.junit.Test;

/**
 * @author Jan Schnasse
 * 
 */
@SuppressWarnings("javadoc")
public class OaiDcFlowTest {
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
				.process("<http://lobid.org/resource/TT002234391> "
						+ "<http://xmlns.com/foaf/0.1/isPrimaryTopicOf> "
						+ "<http://193.30.112.134/F/?func=find-c&ccl_term=IDN%3DTT002234391> .");
		decoder.closeStream();
	}

	@Test
	public void testFlow() {
		final HttpOpener opener = new HttpOpener();
		opener.setAccept("text/plain");
		final StreamToStringReader reader = new StreamToStringReader();
		final NTripleDecoder decoder = new NTripleDecoder();
		final StreamLogger logger = new StreamLogger("decoder");
		final Metamorph metamorph = new Metamorph("morph-lobid-to-oaidc.xml");
		final ObjectStdoutWriter<String> writer = new ObjectStdoutWriter<String>();
		final StreamTee tee = new StreamTee();
		final OaiDcEncoder encoder = new OaiDcEncoder();
		metamorph.setReceiver(encoder).setReceiver(writer);
		tee.addReceiver(logger);
		tee.addReceiver(metamorph);
		opener.setReceiver(reader);
		reader.setReceiver(decoder);
		decoder.setReceiver(tee);
		opener.process("http://api.lobid.org/resource?id=HT015381412");
		opener.closeStream();
	}

	@Test
	public void testFlux() throws IOException, URISyntaxException,
			RecognitionException {
		String lobidUrl = "http://api.lobid.org/resource?id=HT015381412";
		File fluxFile =
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("morph-lobid-to-oaidc.flux").toURI());
		Flux.main(new String[] { fluxFile.getAbsolutePath(), "url=" + lobidUrl,
				"out=" + "stdout" });
	}
}
