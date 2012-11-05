/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.sparql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.culturegraph.semanticweb.sink.AbstractModelWriter.Format;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.CharStreams;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Test {@link FourStore} using the lobid.org instance.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class FourStoreTests {

	private static final String URL = "http://aither.hbz-nrw.de:8000";
	private static final String ENCODING = "UTF-8";
	private static final String LOBID = "lobid-resource_base_small.nt";
	private static final Logger LOG = LoggerFactory
			.getLogger(FourStoreTests.class);
	private final FourStore store = new FourStore(URL);

	@Test
	public void sparqlSimple() throws IOException {
		/* Most simple way to send a SPARQL query: use a URL object */
		final URL url =
				new URL(URL
						+ "/sparql/"
						+ "?query="
						+ URLEncoder.encode(
								"SELECT * FROM <http://example.com/G>"
										+ " WHERE { ?s ?p ?o } LIMIT 50",
								ENCODING));
		final String s =
				CharStreams.toString(new InputStreamReader(url.openStream(),
						"UTF-8"));
		assertFalse("Output string should contain something", s.trim()
				.isEmpty());
	}

	@Test
	public void read() throws URISyntaxException, IOException {
		Model model = ModelFactory.createDefaultModel();
		final URL url = findUrl(LOBID);
		model = model.read(url.toString(), Format.N_TRIPLE.getName());
		assertTrue("Resulting graph should contain triples", model.getGraph()
				.size() > 0);
	}

	@Test
	public void readAll() throws URISyntaxException {
		final Graph graph = loadGraph();
		final Triple any = Triple.createMatch(null, null, null);
		final List<Triple> triples = graph.find(any).toList();
		assertEquals("The number of triples should equal the graph size",
				triples.size(), graph.size());
		LOG.info(triples.size() + " triples loaded");
	}

	@Test
	public void update() throws IOException {
		final String graph = "http://example.com/G";
		final HttpResponse delete = store.deleteGraph(graph);
		LOG.info(delete.toString());
		final HttpResponse response =
				store.insertTriple(graph, Triple.create(
						Node.createURI("http://example.com/s"),
						Node.createURI("http://example.com/p"),
						Node.createLiteral("o")));
		assertEquals("Response should indicate status OK", HttpStatus.SC_OK,
				response.getStatusLine().getStatusCode());
		final List<QuerySolution> result =
				store.sparqlSelect(String.format(
						"SELECT * FROM <%s> WHERE { ?s ?p ?o } LIMIT 100",
						graph));
		assertEquals("New triple should be in named graph", 1, result.size());

	}

	private Graph loadGraph() {
		Model model = ModelFactory.createDefaultModel();
		final URL url = findUrl(LOBID);
		model = model.read(url.toString(), Format.N_TRIPLE.getName());
		return model.getGraph();
	}

	private URL findUrl(final String name) {
		return Thread.currentThread().getContextClassLoader().getResource(name);
	}
}
