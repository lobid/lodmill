/* Copyright 2013 hbz. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.File;

import junit.framework.Assert;

import org.junit.Test;

/**
 * Ingest the ZDB ISIL Authority File PICA-XML .
 * 
 * Run as JUnit test to print some stats, transform the fields, and output
 * results as N-Triples.
 * 
 * @author Pascal Christoph (dr0i)
 */
@SuppressWarnings("javadoc")
public final class LobidOrganisationPicaIngest extends Ingest {

	public LobidOrganisationPicaIngest() {
		super("transformations/lobid-organisation/Bibdat1303pp_sample1.xml",
				"morph_zdb-isil-file-pica2ld.xml", "zvdd_morph-stats.xml",
				new PicaXmlReader());
	}

	@Test
	public void triples() {
		setUpErrorHandler(metamorph);
		final File file = new File("zdb-isil-file.nt");
		process(new GeolocationLookupTriplesEncoder(), file);
		Assert.assertTrue(file.exists());
	}

	@Test
	public void dot() {
		setUpErrorHandler(metamorph);
		final File file = new File("zdb-isil-file.dot");
		process(new PipeEncodeDot(), file);
		Assert.assertTrue(file.exists());
	}

}
