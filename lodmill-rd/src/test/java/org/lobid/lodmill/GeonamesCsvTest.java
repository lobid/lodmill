/* Copyright 2013 hbz. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.IOException;

import org.culturegraph.mf.stream.reader.CsvReader;
import org.junit.Test;

/**
 * Ingest the geonames tsv dump.
 * 
 * Run as JUnit test to print some stats, transform the fields and output
 * results as N-Triples and graphiz dot file.
 * 
 * @author Pascal Christoph (dr0i)
 */
@SuppressWarnings("javadoc")
public final class GeonamesCsvTest extends AbstractIngestTests {

	public GeonamesCsvTest() {
		super("src/main/resources/geonames_DE_sample.csv",
				"morphGeonamesCsv2ld.xml", "default_morph-stats.xml", new CsvReader(
						"\t"));
	}

	@Test
	public void testTriples() { // NOPMD asserts are done in the superclass
		super.triples("geonames_test.nt", "geonames.nt", new PipeEncodeTriples());
	}

	@Test
	public void testStatistics() throws IOException { // NOPMD
		super.stats("mapping.textile");
	}

	@Test
	public void testDot() { // NOPMD
		super.dot("zdb-isil-file_test.dot");
	}
}