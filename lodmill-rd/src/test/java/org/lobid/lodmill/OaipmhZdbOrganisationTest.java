/* Copyright 2013 Jan Schnasse, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.sink.ObjectWriter;
import org.junit.Test;

/**
 * @author Fabian Steeg
 * @author Pascal Christoph
 * 
 */
@SuppressWarnings("javadoc")
public class OaipmhZdbOrganisationTest {

	@Test
	public void testFlow() {
		final OaiPmhOpener opener = new OaiPmhOpener();
		opener.setDateFrom("2013-08-11");
		opener.setDateUntil("2013-08-12");
		opener.setMetadataPrefix("PicaPlus-xml");
		opener.setSetSpec("bib");
		final PicaXmlReader reader = new PicaXmlReader();
		final Metamorph metamorph =
				new Metamorph("morph_zdb-isil-file-pica2ld.xml");
		final PipeLobidOrganisationEnrichment enrich =
				new PipeLobidOrganisationEnrichment();
		enrich.setGeonameFilename("geonames_DE_sample.csv");
		enrich.doApiLookup = false;
		enrich.setSerialization("TURTLE");
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
