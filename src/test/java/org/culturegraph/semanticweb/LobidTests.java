/* Copyright 2012 Fabian Steeg. Licensed under the Apache License Version 2.0 */

package org.culturegraph.semanticweb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import org.culturegraph.metastream.sink.StreamWriter;
import org.culturegraph.metastream.source.FileOpener;
import org.culturegraph.semanticweb.pipe.JenaModel;
import org.culturegraph.semanticweb.pipe.JenaModelToStream;
import org.culturegraph.semanticweb.sink.AbstractModelWriter.Format;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Process lobid.org data.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class LobidTests {

	private static final String LOBID_NT = "lobid-resource_base_small.nt";
	private static final String LOBID_XML = "out/" + LOBID_NT + ".xml";
	private static final Logger LOG = LoggerFactory.getLogger(LobidTests.class);

	@BeforeClass
	public static void convert() throws FileNotFoundException {
		Model model = ModelFactory.createDefaultModel();
		model = model.read(findUrl(LOBID_NT).toString(),
				Format.N_TRIPLE.getName());
		final File file = new File(LOBID_XML);
		model.write(new FileOutputStream(file), Format.RDF_XML.getName());
		assertTrue("Output file should exist", file.exists());
	}

	@Test
	public void readAll() throws URISyntaxException {
		final FileOpener opener = new FileOpener();
		final Graph graph = loadGraph(opener);
		final Triple any = Triple.createMatch(null, null, null);
		final List<Triple> triples = graph.find(any).toList();
		assertEquals("The number of triples should equal the graph size",
				triples.size(), graph.size());
		LOG.info(triples.size() + " triples loaded");
		opener.closeStream();
	}

	private Graph loadGraph(final FileOpener opener) throws URISyntaxException {
		final JenaModel model = new JenaModel();
		opener.setReceiver(model)
				.setReceiver(new JenaModelToStream())
				.setReceiver(
						new StreamWriter(new OutputStreamWriter(System.out)));
		opener.process(new File(LOBID_XML).getAbsolutePath());
		return model.getModel().getGraph();
	}

	private static URL findUrl(final String name) {
		return Thread.currentThread().getContextClassLoader().getResource(name);
	}
}
