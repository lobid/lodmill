/* Copyright 2012 Fabian Steeg. Licensed under the Apache License Version 2.0 */

package org.culturegraph.semanticweb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.culturegraph.semanticweb.data.FourStore;
import org.culturegraph.semanticweb.data.Gutenberg;
import org.culturegraph.semanticweb.sink.AbstractModelWriter.Format;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Combine lobid.org, Gutenberg, and GND data, e.g. to get works at Gutenberg
 * written by the author of a lobid.org resource ('Works by this author at
 * Gutenberg').
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class GutenbergTests {

	private static final String CREATORS = "creators_small.nt";
	private static final String GRAPH = "http://lobid.org/graph/gutenberg/gnd";
	private static final String OUT = "out/new.nt";
	private static final String MAP_BIN = "out/map.bin";
	private static final Logger LOG = LoggerFactory
			.getLogger(GutenbergTests.class);
	/* First run, pass '-Xmx3000m -XX:+UseConcMarkSweepGC' as JVM args for Jena */
	private static final Gutenberg GUTENBERG = new Gutenberg(new File(MAP_BIN));
	private final FourStore store = new FourStore(
			"http://aither.hbz-nrw.de:8000");

	@Test
	public void gutenbergWorksForLobidAuthor() throws URISyntaxException,
			IOException {
		final Set<String> gndIds = gndAuthorIds(CREATORS);
		LOG.info(String.format("Got %s author ids", gndIds.size()));
		final Writer writer = new FileWriter(new File(OUT));
		final Model newModel = GUTENBERG.linkGutenbergToGndAuthors(gndIds,
				store, writer);
		writer.close();
		LOG.info(String.format("Created new model, size %s", newModel.size()));
		assertTrue("New triples should have been found", newModel.getGraph()
				.size() > 0);
		/* Upload and use our small testing data: */
		upload(OUT, store, GRAPH);
		newModelSampleUsage(gndIds, newModel.getGraph());
		/* Do something useful with full data (see also data-info.textile): */
		// upload("out/new_full.nt", store /* or into production */, GRAPH);
	}

	/**
	 * @param file The file containing the model to store
	 * @param store The store to write the model to
	 * @param graph The name of the graph to store the model
	 * @throws IOException If we can't read the input file or write to the store
	 */
	public void upload(final String file, final FourStore store,
			final String graph) throws IOException {
		final Model model = ModelFactory.createDefaultModel();
		model.read(new FileReader(file), null, Format.N_TRIPLE.getName());
		final Triple any = Triple.createMatch(null, null, null);
		final List<Triple> triples = model.getGraph().find(any).toList();
		for (Triple triple : triples) {
			final HttpResponse response = store.insertTriple(graph, triple);
			LOG.info(String.format("Insert triple %s, response %s", triple,
					response));
			assertEquals("Response should be OK", HttpStatus.SC_OK, response
					.getStatusLine().getStatusCode());
		}
		LOG.info(triples.size() + " triples inserted into graph " + graph);
		final List<QuerySolution> result = store.sparqlSelect("SELECT * FROM "
				+ "<" + graph + ">" + " WHERE { ?s ?p ?o } LIMIT 5000");
		assertFalse("Should have uploaded some triples", result.isEmpty());
	}

	/**
	 * @param authors The file with creator n-triples
	 * @return The unique GND author ID, picked out of the objects in the file
	 */
	public Set<String> gndAuthorIds(final String authors) {
		final Scanner scanner = new Scanner(Thread.currentThread()
				.getContextClassLoader().getResourceAsStream(authors));
		final Set<String> ids = new HashSet<String>();
		while (scanner.hasNextLine()) {
			final String line = scanner.nextLine();
			final String gndId = line.substring(line.lastIndexOf('<') + 1,
					line.lastIndexOf('>'));
			if (gndId.startsWith("http://d-nb.info/gnd/")) {
				ids.add(gndId);
			}
		}
		scanner.close();
		LOG.info("Unique authors: " + ids.size());
		return ids;
	}

	private void newModelSampleUsage(final Set<String> gndIds,
			final Graph newGraph) {
		for (String gndId : gndIds) {
			final List<Triple> find = newGraph.find(
					Triple.createMatch(null, Node.createURI(Gutenberg.CREATOR),
							Node.createURI(gndId))).toList();
			if (!find.isEmpty()) {
				LOG.info(String
						.format("\nWorks at Gutenberg by %s (author of a lobid entry):\n",
								gndId));
				for (Triple newTriple : find) {
					LOG.info(newTriple.getSubject().toString());
				}
			}
		}
	}

}
