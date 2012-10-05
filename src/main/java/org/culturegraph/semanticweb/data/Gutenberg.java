/* Copyright 2012 Fabian Steeg. Licensed under the Apache License Version 2.0 */

package org.culturegraph.semanticweb.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.culturegraph.semanticweb.sink.AbstractModelWriter.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Combine Gutenberg and GND data to link resources on Gutenberg to GND authors. <br/>
 * Gutenberg uses literals for authors, containing the full name and the years
 * of birth and death. To link Gutenberg works with GND ids, we look for GND
 * authors with matching name and life dates. If both the name and the dates
 * match, we assume the author to be identical.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class Gutenberg {

	public static final String CREATOR = "http://purl.org/dc/elements/1.1/creator";
	private static final String DEATH = "http://d-nb.info/standards/elementset/gnd#dateOfDeath";
	private static final String BIRTH = "http://d-nb.info/standards/elementset/gnd#dateOfBirth";
	private static final String NAME = "http://d-nb.info/standards/elementset/gnd#preferredNameForThePerson";
	private static final String GUTENBERG = "http://www.gutenberg.org/feeds/catalog.rdf.zip";
	private static final Logger LOG = LoggerFactory.getLogger(Gutenberg.class);
	private static final int PROGRESS_STEPS = 1000;
	private final Map<String, String> authorMap;

	/**
	 * @param serializedAuthorMap The file to cache the author mapping to avoid
	 *            costly processing of the Gutenberg catalogue on every run. The
	 *            map is loaded from the given file, or created and stored in
	 *            the file, if it does not exist yet.
	 */
	public Gutenberg(final File serializedAuthorMap) {
		this.authorMap = map(serializedAuthorMap);
	}

	/**
	 * @param gndIds The set of authors to link up with Gutenberg
	 * @param store The store to check for information about the authors
	 * @param writer The destination to persist the linked model
	 * @return The resulting model, linking Gutenberg works to GND authors
	 */
	public Model linkGutenbergToGndAuthors(final Set<String> gndIds,
			final FourStore store, final Writer writer) {
		final Model model = ModelFactory.createDefaultModel();
		int count = 1;
		for (String gndId : gndIds) {
			/*
			 * When we have a few million authors, this can get quite long, so
			 * we want some kind of progress indicator:
			 */
			if (count % PROGRESS_STEPS == 0) {
				LOG.info(count + "...");
			}
			count = count + 1;
			findTriples(store, writer, model, gndId);
		}
		return model;
	}

	private void findTriples(final FourStore store, final Writer writer,
			final Model newModel, final String gndId) {
		for (String key : getKeys(gndId, store)) {
			final String val = authorMap.get(key);
			if (val != null) {
				final Triple newTriple = Triple.create(Node.createURI(val),
						Node.createURI(CREATOR), Node.createURI(gndId));
				LOG.info("New triple: " + newTriple);
				/*
				 * We write the model after every new triple that was found to
				 * be able to continue later if the process dies halfway
				 * through:
				 */
				newModel.write(writer, Format.N_TRIPLE.getName());
				newModel.getGraph().add(newTriple);
			}
		}
	}

	private Map<String, String> map(final File serializedMap) {
		return serializedMap.exists() ? readMap(serializedMap)
				: writeMap(serializedMap);
	}

	public static List<String> getKeys(final String gndPersonId,
			final FourStore store) {
		final Graph graph = store.sparqlConstruct(String.format(
				"CONSTRUCT { <%s> ?p ?o } WHERE { <%s> ?p ?o ."
						+ "FILTER ( ?p=<%s> || ?p=<%s> || ?p=<%s> ) }",
				gndPersonId, gndPersonId, NAME, BIRTH, DEATH));
		final String lifeDates = lifeDates(graph);
		final List<String> result = new ArrayList<String>();
		for (Triple name : triplesWithPredicate(NAME, graph)) {
			// e.g. "Flygare-Carlen, Emilie, 1807-1892"
			final String key = String.format("%s%s", name.getObject()
					.getLiteralValue(), lifeDates);
			result.add(key);
		}
		return result;
	}

	private static String lifeDates(final Graph graph) {
		final List<Triple> birth = triplesWithPredicate(BIRTH, graph);
		final List<Triple> death = triplesWithPredicate(DEATH, graph);
		final String birthString = birth.isEmpty() ? "" : String.format(
				", %s-", birth.get(0).getObject().getLiteralValue());
		final String deathString = death.isEmpty() ? "" : death.get(0)
				.getObject().getLiteralValue().toString();
		return birthString + deathString;
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

	private static List<Triple> triplesWithPredicate(final String predicate,
			final Graph graph) {
		return graph.find(
				Triple.createMatch(null, Node.createURI(predicate), null))
				.toList();
	}

	private Map<String, String> writeMap(final File serializedMap) {
		try {
			final Map<String, String> map = mapLiteralsToGutenbergIds();
			final ObjectOutputStream stream = new ObjectOutputStream(
					new FileOutputStream(serializedMap));
			stream.writeObject(map);
			stream.close();
			return map;
		} catch (FileNotFoundException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return null;
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

	private Map<String, String> readMap(final File serializedMap) {
		try {
			final ObjectInputStream stream = new ObjectInputStream(
					new FileInputStream(serializedMap));
			@SuppressWarnings("unchecked")
			final Map<String, String> map = (Map<String, String>) stream
					.readObject();
			stream.close();
			return map;
		} catch (FileNotFoundException e) {
			LOG.error(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return null;
	}
}
