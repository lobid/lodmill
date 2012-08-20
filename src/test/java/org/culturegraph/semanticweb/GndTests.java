/* Copyright 2012 Fabian Steeg. Licensed under the Apache License Version 2.0 */

package org.culturegraph.semanticweb;

import static org.junit.Assert.assertEquals;

import java.io.OutputStreamWriter;
import java.util.List;

import org.culturegraph.metastream.sink.StreamWriter;
import org.culturegraph.metastream.source.HttpOpener;
import org.culturegraph.semanticweb.pipe.JenaModel;
import org.culturegraph.semanticweb.pipe.JenaModelToStream;
import org.junit.Test;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

/**
 * Process GND data.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class GndTests {

	private static final String SUBJECT = "http://d-nb.info/gnd/121649091";
	private static final String RDF_URL = SUBJECT + "/about/rdf";

	@Test
	public void readAll() {
		final HttpOpener httpOpener = new HttpOpener();
		final Graph graph = loadGraph(httpOpener);
		final Triple any = Triple.createMatch(null, null, null);
		final List<Triple> triples = graph.find(any).toList();
		assertEquals("The graph size is the number of triples", triples.size(),
				graph.size());
		httpOpener.closeStream();
	}

	@Test
	public void readSubject() {
		final HttpOpener httpOpener = new HttpOpener();
		final Graph graph = loadGraph(httpOpener);
		final List<Triple> triples = graph.find(Node.createURI(SUBJECT), null,
				null).toList();
		final Node subject = triples.get(0).getSubject();
		for (Triple triple : triples) {
			assertEquals("Triple should have same subject", subject,
					triple.getSubject());
		}
		httpOpener.closeStream();
	}

	private Graph loadGraph(final HttpOpener httpOpener) {
		final JenaModel model = new JenaModel();
		httpOpener
				.setReceiver(model)
				.setReceiver(new JenaModelToStream())
				.setReceiver(
						new StreamWriter(new OutputStreamWriter(System.out)));
		httpOpener.process(RDF_URL);
		return model.getModel().getGraph();
	}

}
