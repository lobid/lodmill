/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.File;

import junit.framework.Assert;

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
public final class ZvddMarcIngest extends Ingest {

	public ZvddMarcIngest() {
		super("../../zvdd.xml", "morph_zvdd-title-digital2ld.xml",
				"zvdd_morph-stats.xml", new MarcXmlReader());
	}

	@Test
	public void triples() {
		setUpErrorHandler(metamorph);
		final File file = new File("zvdd-title-digitalisation.nt");
		process(new PipeEncodeTriples(), file);
		Assert.assertTrue(file.exists());
	}

	@Test
	public void dot() {
		setUpErrorHandler(metamorph);
		final File file = new File("zvdd-title-digitalisation.dot");
		process(new PipeEncodeDot(), file);
		Assert.assertTrue(file.exists());
	}

}
