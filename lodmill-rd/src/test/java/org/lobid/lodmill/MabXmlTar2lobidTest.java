/* Copyright 2013  hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.pipe.ObjectTee;
import org.culturegraph.mf.stream.pipe.StreamTee;
import org.culturegraph.mf.stream.sink.ObjectStdoutWriter;
import org.culturegraph.mf.stream.source.FileOpener;
import org.junit.Test;

/**
 * @author Pascal Christoph
 * 
 */
@SuppressWarnings("javadoc")
public final class MabXmlTar2lobidTest {

	@SuppressWarnings("static-method")
	@Test
	public void testFlow() throws IOException, URISyntaxException {
		final String targetPath = "tmp";
		final FileOpener opener = new FileOpener();
		opener.setCompression("BZIP2");
		TarReader tarReader = new TarReader();
		final XmlDecoder xmlDecoder = new XmlDecoder();
		final MabXmlHandler handler = new MabXmlHandler();
		final Metamorph morph = new Metamorph("morph-hbz01-to-lobid.xml");
		final Triples2RdfModel triple2model = new Triples2RdfModel();
		triple2model.setInput("N-TRIPLE");
		final ObjectTee<String> tee = new ObjectTee<String>();
		RdfModelFileWriter modelWriter = new RdfModelFileWriter();
		modelWriter.setProperty("http://lobid.org/vocab/lobid#hbzID");
		modelWriter.setSerialization("N-TRIPLES");
		modelWriter.setStartIndex(2);
		modelWriter.setEndIndex(7);
		String targetDirectory = "tmp/nt";
		modelWriter.setTarget(targetDirectory);
		triple2model.setReceiver(modelWriter);
		final ObjectStdoutWriter<String> writer = new ObjectStdoutWriter<String>();
		tee.addReceiver(writer);
		tee.addReceiver(triple2model);
		StreamTee streamTee = new StreamTee();
		final Stats stats = new Stats();
		streamTee.addReceiver(stats);
		PipeEncodeTriples encoder = new PipeEncodeTriples();
		streamTee.addReceiver(encoder);
		encoder.setReceiver(tee);
		XmlTee xmlTee = new XmlTee();
		XmlEntitySplitter xmlEntitySplitter = new XmlEntitySplitter();
		xmlEntitySplitter.setEntityName("ListRecords");
		XmlFilenameWriter xmlFilenameWriter = new XmlFilenameWriter();
		xmlFilenameWriter.setStartIndex(2);
		xmlFilenameWriter.setEndIndex(7);
		xmlFilenameWriter.setTarget("tmp/xml");
		xmlFilenameWriter
				.setProperty("/OAI-PMH/ListRecords/record/metadata/record/datafield[@tag='001']/subfield[@code='a']");
		xmlFilenameWriter.setCompression("bz2");
		xmlFilenameWriter.setFileSuffix("");
		xmlFilenameWriter.setEncoding("utf8");
		xmlTee.setReceiver(handler).setReceiver(morph).setReceiver(streamTee);
		xmlTee.addReceiver(xmlEntitySplitter);
		xmlEntitySplitter.setReceiver(xmlFilenameWriter);
		opener.setReceiver(tarReader).setReceiver(xmlDecoder).setReceiver(xmlTee);
		File infile = new File("src/test/resources/hbz01XmlClobs.tar.bz2");// "/home/data/demeter/alephxml/clobs/update/20131101_20131102.tar.bz2");
		opener.process(infile.getAbsolutePath());
		Stats.writeTextileMappingTable(stats.sortedByValuesDescending(), new File(
				"/dev/null"));
		opener.closeStream();
	}
}
