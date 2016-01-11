/* Copyright 2015  hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.source.FileOpener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.node.Node;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.jena.JenaTripleCallback;
import com.github.jsonldjava.utils.JSONUtils;
import com.hp.hpl.jena.rdf.model.Model;

import de.hbz.lobid.helper.RdfUtils;

/**
 * Transform hbz01 Aleph Mab XML catalog data into lobid elasticsearch JSON-LD.
 * Query the index and test the data by transforming the data into ntriples
 * (which is great to make diffs).
 * 
 * @author Pascal Christoph (dr0i)
 * 
 */
@SuppressWarnings("javadoc")
public final class MabXml2ElasticsearchLobidTest {
	private static final Logger LOG =
			LoggerFactory.getLogger(MabXml2ElasticsearchLobidTest.class);
	private static Node node;
	protected static Client client;
	private static final String LOBID_RESOURCES =
			"lobid-resources-" + LocalDateTime.now().toLocalDate() + "-"
					+ LocalDateTime.now().toLocalTime();
	private static final String N_TRIPLE = "N-TRIPLE";
	private static final String TEST_FILENAME = "hbz01.es.nt";

	@BeforeClass
	public static void setup() {
		node = nodeBuilder().local(true)
				.settings(ImmutableSettings.settingsBuilder()
						.put("index.number_of_replicas", "0")
						.put("index.number_of_shards", "1").build())
				.node();
		client = node.client();
		client.admin().indices().prepareDelete("_all").execute().actionGet();
		client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute()
				.actionGet();
	}

	@SuppressWarnings("static-method")
	@Test
	public void testFlow_with_new_Converter() {
		commonTestRoutine(new RdfModel2ElasticsearchEtikettJsonLd(), ".new");
	}

	@SuppressWarnings("static-method")
	@Test
	public void testFlow_with_old_Converter() {
		commonTestRoutine(new RdfModel2ElasticsearchJsonLd(), ".old");
	}

	private static void commonTestRoutine(
			DefaultObjectPipe<Model, ObjectReceiver<HashMap<String, String>>> jsonConverter,
			String suffix) {
		try {
			File testFile = new File("src/test/resources/" + TEST_FILENAME + suffix);
			buildAndExecuteFlow(client, jsonConverter);
			String ntriples = getElasticsearchDocumentsAsNtriples();
			FileUtils.writeStringToFile(testFile, ntriples, false);
			String actualRdfString = getRdfString(testFile.getName());
			String expectedRdfString = getRdfString(TEST_FILENAME);
			boolean result = rdfCompare(actualRdfString, expectedRdfString);
			AbstractIngestTests.compareFilesDefaultingBNodes(testFile,
					new File(Thread.currentThread().getContextClassLoader()
							.getResource(TEST_FILENAME).toURI()));
			org.junit.Assert.assertTrue(result);
			// if everything is ok - delete the output files
			testFile.deleteOnExit();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static String getRdfString(String rdfFilename) {
		LOG.info("Read rdf " + rdfFilename);
		try (InputStream in = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream(rdfFilename)) {
			String rdfString = RdfUtils.readRdfToString(in, RDFFormat.NTRIPLES,
					RDFFormat.NTRIPLES, "");
			return rdfString;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static boolean rdfCompare(String actual, String expected) {
		String actualWithoutBlankNodes = removeBlankNodes(actual);
		String expectedWithoutBlankNodes = removeBlankNodes(expected);
		String[] actualSorted = sorted(actualWithoutBlankNodes);
		String[] expectedSorted = sorted(expectedWithoutBlankNodes);
		return findErrors(actualSorted, expectedSorted);
	}

	private static boolean findErrors(String[] actualSorted,
			String[] expectedSorted) {
		boolean result = true;
		if (actualSorted.length != expectedSorted.length) {
			LOG.error("Expected size of " + expectedSorted.length
					+ " is different to actual size " + actualSorted.length);
		} else {
			LOG.info("Expected size of " + expectedSorted.length
					+ " is different to actual size " + actualSorted.length);
		}
		for (int i = 0; i < actualSorted.length; i++) {
			if (actualSorted[i].equals(expectedSorted[i])) {
				LOG.debug("Actual , Expected");
				LOG.debug(actualSorted[i]);
				LOG.debug(expectedSorted[i]);
				LOG.debug("");
			} else {
				LOG.error("Error line " + i);
				LOG.error("Actual , Expected");
				LOG.error(actualSorted[i]);
				LOG.error(expectedSorted[i]);
				LOG.error("");
				result = false;
				break;
			}
		}
		return result;
	}

	private static String removeBlankNodes(String str) {
		return str.replaceAll("_:[^\\ ]*", "")
				.replaceAll("\\^\\^<http://www.w3.org/2001/XMLSchema#string>", "");
	}

	private static String[] sorted(String actualWithoutBlankNodes) {
		String[] list = createList(actualWithoutBlankNodes);
		ArrayList<String> words = new ArrayList<>(Arrays.asList(list));
		Collections.sort(words, String.CASE_INSENSITIVE_ORDER);
		LOG.debug(words.toString());
		String[] ar = new String[words.size()];
		return words.toArray(ar);
	}

	private static String[] createList(String actualWithoutBlankNodes) {
		try (BufferedReader br =
				new BufferedReader(new StringReader(actualWithoutBlankNodes))) {
			List<String> result = new ArrayList<>();
			String line;
			while ((line = br.readLine()) != null) {
				result.add(line);
			}
			String[] ar = new String[result.size()];
			return result.toArray(ar);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void buildAndExecuteFlow(final Client cl,
			DefaultObjectPipe<Model, ObjectReceiver<HashMap<String, String>>> jsonConverter) {
		final FileOpener opener = new FileOpener();
		opener.setCompression("BZIP2");
		final Triples2RdfModel triple2model = new Triples2RdfModel();
		triple2model.setInput(N_TRIPLE);
		opener.setReceiver(new TarReader()).setReceiver(new XmlDecoder())
				.setReceiver(new MabXmlHandler())
				.setReceiver(
						new Metamorph("src/main/resources/morph-hbz01-to-lobid.xml"))
				.setReceiver(new PipeEncodeTriples()).setReceiver(triple2model)
				.setReceiver(jsonConverter).setReceiver(getElasticsearchIndexer(cl));
		opener.process(
				new File("src/test/resources/hbz01XmlClobs.tar.bz2").getAbsolutePath());
		opener.closeStream();
	}

	private static ElasticsearchIndexer getElasticsearchIndexer(
			final Client _client) {
		ElasticsearchIndexer esIndexer = new ElasticsearchIndexer();
		esIndexer.setElasticsearchClient(_client);
		esIndexer.setIndexName(LOBID_RESOURCES);
		esIndexer.setIndexAliasSuffix("");
		esIndexer.setUpdateNewestIndex(false);
		esIndexer.onSetReceiver();
		return esIndexer;
	}

	private static String getElasticsearchDocumentsAsNtriples() {
		SearchResponse actionGet = client.prepareSearch(LOBID_RESOURCES)
				.setQuery(new MatchAllQueryBuilder()).setFrom(0).setSize(10000)
				.execute().actionGet();
		return Arrays.asList(actionGet.getHits().getHits()).parallelStream()
				.flatMap(hit -> Stream.of(toRdf(hit.getSourceAsString())))
				.collect(Collectors.joining());
	}

	private static String toRdf(final String jsonLd) {
		try {
			final Object jsonObject = JSONUtils.fromString(jsonLd);
			final JenaTripleCallback callback = new JenaTripleCallback();
			final Model model = (Model) JsonLdProcessor.toRDF(jsonObject, callback);
			final StringWriter writer = new StringWriter();
			model.write(writer, N_TRIPLE);
			return writer.toString();
		} catch (IOException | JsonLdError e) {
			e.printStackTrace();
		}
		return null;
	}

	@AfterClass
	public static void down() {
		// client.admin().indices().prepareDelete("_all").execute().actionGet();
		// node.close();
	}
}
