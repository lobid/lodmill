/* Copyright 2015  hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
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

import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.jena.JenaTripleCallback;
import com.github.jsonldjava.utils.JSONUtils;
import com.hp.hpl.jena.rdf.model.Model;

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

	private static void commonTestRoutine(
			DefaultObjectPipe<Model, ObjectReceiver<HashMap<String, String>>> jsonConverter,
			String suffix) {
		try {
			File testFile = new File("src/test/resources/" + TEST_FILENAME + suffix);
			buildAndExecuteFlow(client, jsonConverter);
			String ntriples = getElasticsearchDocumentsAsNtriples();
			FileUtils.writeStringToFile(testFile, ntriples, false);
			AbstractIngestTests.compareFilesDefaultingBNodes(testFile,
					new File(Thread.currentThread().getContextClassLoader()
							.getResource(TEST_FILENAME).toURI()));
			// if everything is ok - delete the output files
			testFile.deleteOnExit();
		} catch (Exception e) {
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
