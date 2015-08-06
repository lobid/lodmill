/* Copyright 2015  hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill.run;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.pipe.BatchLogger;
import org.culturegraph.mf.stream.pipe.ObjectBatchLogger;
import org.culturegraph.mf.stream.pipe.StreamTee;
import org.culturegraph.mf.stream.source.FileOpener;
import org.culturegraph.mf.stream.source.TarReader;
import org.lobid.lodmill.ElasticsearchIndexer;
import org.lobid.lodmill.MabXmlHandler;
import org.lobid.lodmill.PipeEncodeTriples;
import org.lobid.lodmill.RdfModel2ElasticsearchJsonLd;
import org.lobid.lodmill.Stats;
import org.lobid.lodmill.Triples2RdfModel;

/**
 * Transform hbz01 Aleph Mab XML catalog data into lobid elasticsearch ready
 * JSON-LD and index that into elasticsearch.
 * 
 * @author Pascal Christoph (dr0i)
 * 
 */
@SuppressWarnings("javadoc")
public final class MabXml2lobidJsonEs {

	public static void main(String... args) {
		String usage =
				"<input path>%s<index name>%s<index alias suffix>%s<node>%s<cluster>%s<'update' (will take latest index), 'exact' (will take ->'index name' even when no timestamp is suffixed) , else create new index with actual timestamp>%s";
		String inputPath = args[0];
		String indexName = args[1];
		String date = new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date());
		indexName =
				indexName.matches(".*-20.*") || args[5].toLowerCase().equals("exact")
						? indexName : indexName + "-" + date;

		String indexAliasSuffix = args[2];
		String node = args[3];
		String cluster = args[4];
		boolean update = args[5].toLowerCase().equals("update");
		System.out.println(
				"It is specified:\n" + String.format(usage, ": " + inputPath + "\n",
						": " + indexName + "\n", ": " + indexAliasSuffix + "\n",
						": " + node + "\n", ": " + cluster, ": " + "\n" + update));
		if (args.length != 6) {
			System.err.println("Usage: MabXml2lobidJsonEs"
					+ String.format(usage, " ", " ", " ", " ", " ", " "));
			System.exit(-1);
		}

		// hbz catalog transformation
		final FileOpener opener = new FileOpener();
		if (inputPath.toLowerCase().endsWith("bz2")) {
			opener.setCompression("BZIP2");
		} else if (inputPath.toLowerCase().endsWith("gz"))
			opener.setCompression("GZIP");
		final Triples2RdfModel triple2model = new Triples2RdfModel();
		triple2model.setInput("N-TRIPLE");
		ElasticsearchIndexer esIndexer = new ElasticsearchIndexer();
		esIndexer.setClustername(cluster);
		esIndexer.setHostname(node);
		esIndexer.setIndexName(indexName);
		esIndexer.setIndexAliasSuffix(indexAliasSuffix);
		esIndexer.setUpdateNewestIndex(update);
		esIndexer.onSetReceiver();
		BatchLogger batchLogger = new BatchLogger();
		batchLogger.setBatchSize(100000);
		ObjectBatchLogger<HashMap<String, String>> objectBatchLogger =
				new ObjectBatchLogger<>();
		objectBatchLogger.setBatchSize(500000);
		StreamTee streamTee = new StreamTee();
		final Stats stats = new Stats();
		streamTee.addReceiver(stats);
		streamTee.addReceiver(batchLogger);
		batchLogger.setReceiver(new PipeEncodeTriples()).setReceiver(triple2model)
				.setReceiver(new RdfModel2ElasticsearchJsonLd())
				.setReceiver(objectBatchLogger).setReceiver(esIndexer);
		opener.setReceiver(new TarReader()).setReceiver(new XmlDecoder())
				.setReceiver(new MabXmlHandler())
				.setReceiver(
						new Metamorph("src/main/resources/morph-hbz01-to-lobid.xml"))
				.setReceiver(streamTee);
		opener.process(new File(inputPath).getAbsolutePath());
		opener.closeStream();
	}
}
