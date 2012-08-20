/* Copyright 2012 Fabian Steeg. Licensed under the Apache License Version 2.0 */

package org.culturegraph.semanticweb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import org.culturegraph.metastream.sink.StreamWriter;
import org.culturegraph.metastream.source.FileOpener;
import org.culturegraph.semanticweb.pipe.JenaModel;
import org.culturegraph.semanticweb.pipe.JenaModelToStream;
import org.culturegraph.semanticweb.sink.AbstractModelWriter.Format;
import org.junit.Ignore;
import org.junit.Test;

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

	private static final String LOBID = "lobid-resource_base_small.nt";

	@Test
	public void read() throws URISyntaxException, IOException {
		Model model = ModelFactory.createDefaultModel();
		final URL url = findUrl(LOBID);
		model = model.read(url.toString(), Format.N_TRIPLE.getName());
		assertTrue("Resulting graph should contain triples", model.getGraph()
				.size() > 0);
	}

	/* TODO: is there a way to specify the language like above? */
	@Ignore
	@Test
	public void readAll() throws URISyntaxException {
		final FileOpener opener = new FileOpener();
		final Graph graph = loadGraph(opener);
		final Triple any = Triple.createMatch(null, null, null);
		final List<Triple> triples = graph.find(any).toList();
		assertEquals("The number of triples should equal the graph size",
				triples.size(), graph.size());
		System.out.println(triples.size() + " triples loaded");
		opener.closeStream();
	}

	private Graph loadGraph(final FileOpener opener) throws URISyntaxException {
		final JenaModel model = new JenaModel();
		opener.setReceiver(model)
				.setReceiver(new JenaModelToStream())
				.setReceiver(
						new StreamWriter(new OutputStreamWriter(System.out)));
		opener.process(new File(findUrl(LOBID).toURI()).getAbsolutePath());
		return model.getModel().getGraph();
	}

	private URL findUrl(final String name) {
		return Thread.currentThread().getContextClassLoader().getResource(name);
	}

}
