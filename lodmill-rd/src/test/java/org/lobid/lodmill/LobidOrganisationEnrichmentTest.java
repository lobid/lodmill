/* Copyright 2013 hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.IOException;

import org.culturegraph.mf.stream.reader.XmlReaderBase;
import org.junit.Test;

/**
 * Ingest the ZDB ISIL Authority File PICA-XML .
 * 
 * Run as JUnit test to print some stats, transform the fields and output
 * results as N-Triples and graphiz dot file.
 * 
 * @author Pascal Christoph (dr0i)
 */
@SuppressWarnings("javadoc")
public final class LobidOrganisationEnrichmentTest extends AbstractIngestTests {

	public LobidOrganisationEnrichmentTest() {
		super("src/test/resources/Bibdat1303pp_sample1.xml",
				"morph_zdb-isil-file-pica2ld.xml", "default_morph-stats.xml",
				new PicaXmlReader());
	}

	@Test
	public void testTriples() { // NOPMD asserts are done in the superclass
		PipeLobidOrganisationEnrichment enrich =
				new PipeLobidOrganisationEnrichment();
		enrich.setGeonameFilename("geonames_DE_sample.csv");
		enrich.setDoApiLookup(true);
		enrich.setSerialization("TURTLE");
		super.triples("zdb-isil-file_test.ttl", "zdb-isil-file.ttl", enrich);
	}

	@Test
	public void testStatistics() throws IOException { // NOPMD
		super.stats("mapping.textile");
	}

	@Test
	public void testDot() { // NOPMD
		super.dot("zdb-isil-file_test.dot");
	}

	private static class PicaXmlReader extends XmlReaderBase {
		/**
		 * Create a reader for pica XML.
		 */
		public PicaXmlReader() {
			super(new PicaXmlHandler());
		}
	}
}