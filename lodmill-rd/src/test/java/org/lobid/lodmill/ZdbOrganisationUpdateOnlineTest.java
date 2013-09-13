/* Copyright 2013 Jan Schnasse, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.sink.ObjectWriter;
import org.junit.Test;

/**
 * @author Jan Schnasse
 * @author Pascal Christoph
 * 
 */
@SuppressWarnings("javadoc")
public class ZdbOrganisationUpdateOnlineTest {

	@Test
	public void testFlow() {
		System.setProperty(
				PipeLobidOrganisationEnrichment.GEONAMES_DE_FILENAME_SYSTEM_PROPERTY,
				"geonames_DE_sample.csv");
		System.setProperty("doApiLookup", "false");
		final OaiPmhOpener opener =
				new OaiPmhOpener("2013-08-11", "2013-08-12", "PicaPlus-xml", "bib");
		final PicaXmlReader reader = new PicaXmlReader();
		final Metamorph metamorph =
				new Metamorph("morph_zdb-isil-file-pica2ld.xml");
		final PipeLobidOrganisationEnrichment enrich =
				new PipeLobidOrganisationEnrichment();
		final ObjectWriter<String> writer =
				new ObjectWriter<String>(
						"update_zdb-isil-file2lobid-organisations1.ttl");
		opener.setReceiver(reader);
		reader.setReceiver(metamorph);
		metamorph.setReceiver(enrich);
		enrich.setReceiver(writer);
		opener.process("http://services.d-nb.de/oai/repository");
		opener.closeStream();
	}

}
