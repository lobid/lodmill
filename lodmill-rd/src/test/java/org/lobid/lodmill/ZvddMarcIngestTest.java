/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.IOException;

import org.culturegraph.mf.stream.reader.MarcXmlReader;
import org.junit.Test;

/**
 * Ingest the ZVDD MARC-XML export.
 * 
 * Run as Java application to use metaflow definitions; run as JUnit test to
 * print some stats, transform the fields, and output results as N-Triples.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public final class ZvddMarcIngestTest extends AbstractIngestTests {

	public ZvddMarcIngestTest() {
		super("transformations/zvdd/visualization/zvdd-collections-test-set.xml",
				"morph_zvdd-collection2ld.xml", "zvdd_morph-stats.xml",
				new MarcXmlReader());
	}

	@Test
	public void testTriples() { // NOPMD asserts are done in the superclass
		super.triples("zvdd-title-digitalisation_test.nt",
				"zvdd-title-digitalisation.nt", new PipeEncodeTriples());

	}

	@Test
	public void testStatistics() throws IOException { // NOPMD
		super.stats("mapping.textile");
	}

	@Test
	public void testDot() { // NOPMD
		super.dot("zvdd-title-digitalisation_test.dot");
	}
}
