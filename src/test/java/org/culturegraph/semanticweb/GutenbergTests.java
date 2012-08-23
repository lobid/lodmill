/* Copyright 2012 Fabian Steeg. Licensed under the Apache License Version 2.0 */

package org.culturegraph.semanticweb;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.culturegraph.semanticweb.sink.AbstractModelWriter.Format;
import org.junit.Test;

import com.google.common.io.ByteStreams;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Combine lobid.org, Gutenberg, and GND data, e.g. to get works at Gutenberg
 * written by the author of a lobid.org resource ('Works by this author at
 * Gutenberg').
 * 
 * TODO: run with full lobid.org data, store resulting model
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class GutenbergTests {

	private static final String CREATOR = "http://purl.org/dc/elements/1.1/creator";
	private static final String DEATH = "http://d-nb.info/standards/elementset/gnd#dateOfDeath";
	private static final String BIRTH = "http://d-nb.info/standards/elementset/gnd#dateOfBirth";
	private static final String FORENAME = "http://d-nb.info/standards/elementset/gnd#forename";
	private static final String SURNAME = "http://d-nb.info/standards/elementset/gnd#surname";
	private static final String LOBID = "lobid-resource_base_small.nt";
	private static final String GUTENBERG = "http://www.gutenberg.org/feeds/catalog.rdf.zip";

	@Test
	public void gutenbergWorksForLobidAuthor() throws URISyntaxException,
			IOException {
		final Map<String, String> gutenbergAuthors = mapLiteralsToGutenbergIds();
		final List<Triple> lobidTriples = triplesWithPredicate(
				CREATOR,
				ModelFactory
						.createDefaultModel()
						.read(findUrl(LOBID).toString(),
								Format.N_TRIPLE.getName()).getGraph());
		final Model newModel = createNewModel(gutenbergAuthors, lobidTriples);
		assertEquals("Two new triples should have been found", 2, newModel
				.getGraph().size());
		newModelSampleUsage(lobidTriples, newModel.getGraph());
	}

	private void newModelSampleUsage(final List<Triple> lobidTriples,
			final Graph newGraph) {
		for (Triple lobidTriple : lobidTriples) {
			final List<Triple> find = newGraph.find(
					Triple.createMatch(null, lobidTriple.getPredicate(),
							lobidTriple.getObject())).toList();
			if (!find.isEmpty()) {
				System.out.printf(
						"\nWorks at Gutenberg by %s (author of %s):\n",
						lobidTriple.getObject(), lobidTriple.getSubject());
				for (Triple newTriple : find) {
					System.out.println(newTriple.getSubject());
				}
			}
		}
	}

	private URL findUrl(final String name) {
		return Thread.currentThread().getContextClassLoader().getResource(name);
	}

	private Model createNewModel(final Map<String, String> gutenbergAuthors,
			final List<Triple> lobidTriples) {
		final Model newModel = ModelFactory.createDefaultModel();
		for (Triple triple : lobidTriples) {
			final String gndId = triple.getObject().getURI();
			for (String key : getKeys(gndId)) {
				final String val = gutenbergAuthors.get(key);
				if (val != null) {
					final Triple newTriple = Triple.create(Node.createURI(val),
							triple.getPredicate(), triple.getObject());
					System.out.println("New triple: " + newTriple);
					newModel.getGraph().add(newTriple);
				}
			}
		}
		return newModel;
	}

	private List<String> getKeys(final String gndPersonId) {
		final Graph graph = ModelFactory.createDefaultModel()
				.read(gndPersonId, Format.RDF_XML.getName()).getGraph();
		final String lifeDates = lifeDates(graph);
		final List<String> result = new ArrayList<String>();
		for (Triple forename : triplesWithPredicate(FORENAME, graph)) {
			for (Triple surname : triplesWithPredicate(SURNAME, graph)) {
				// e.g. "Flygare-Carlen, Emilie, 1807-1892"
				final String key = String.format("%s, %s%s", surname
						.getObject().getLiteralValue(), forename.getObject()
						.getLiteralValue(), lifeDates);
				result.add(key);
			}
		}
		return result;
	}

	private String lifeDates(final Graph graph) {
		final List<Triple> birth = triplesWithPredicate(BIRTH, graph);
		final List<Triple> death = triplesWithPredicate(DEATH, graph);
		final String birthString = birth.isEmpty() ? "" : String.format(
				", %s-", birth.get(0).getObject().getLiteralValue());
		final String deathString = death.isEmpty() ? "" : death.get(0)
				.getObject().getLiteralValue().toString();
		return birthString + deathString;
	}

	private Map<String, String> mapLiteralsToGutenbergIds() throws IOException {
		final Graph gutenbergGraph = remoteZippedGraph(new URL(GUTENBERG));
		final Map<String, String> authors = new HashMap<String, String>();
		final List<Triple> literals = triplesWithPredicate(CREATOR,
				gutenbergGraph);
		for (Triple triple : literals) {
			final Node object = triple.getObject();
			if (object.isLiteral()) {
				authors.put(object.getLiteralValue().toString(), triple
						.getSubject().getURI());
			}
		}
		return authors;
	}

	private Graph remoteZippedGraph(final URL url) throws IOException {
		final File tempFile = File.createTempFile("temp", ".rdf");
		tempFile.deleteOnExit();
		ByteStreams.copy(url.openStream(), new FileOutputStream(tempFile));
		final ZipFile zipFile = new ZipFile(tempFile);
		final ZipEntry zipEntry = zipFile.entries().nextElement();
		final Graph graph = ModelFactory
				.createDefaultModel()
				.read(zipFile.getInputStream(zipEntry), null,
						Format.RDF_XML.getName()).getGraph();
		zipFile.close();
		return graph;
	}

	private List<Triple> triplesWithPredicate(final String predicate,
			final Graph graph) {
		return graph.find(
				Triple.createMatch(null, Node.createURI(predicate), null))
				.toList();
	}
}
