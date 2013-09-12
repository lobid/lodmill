package org.lobid.lodmill;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.antlr.runtime.RecognitionException;
import org.apache.commons.io.FileUtils;
import org.culturegraph.mf.Flux;
import org.junit.Test;

/**
 * @author Jan Schnasse schnasse@hbz-nrw.de
 * 
 */
@SuppressWarnings("javadoc")
public class DippQdcToLobidTest {

	@Test
	public void testFlux() throws IOException, URISyntaxException,
			RecognitionException {
		String subject = "test:123";
		File outfile = File.createTempFile("lobid", "rdf");
		outfile.deleteOnExit();
		File fluxFile =
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("dipp-qdc-to-lobid.flux").toURI());
		File infile =
				new File(Thread.currentThread().getContextClassLoader()
						.getResource("QDC.xml").toURI());
		Flux.main(new String[] { fluxFile.getAbsolutePath(),
				"in=" + infile.getAbsolutePath(), "out=" + outfile.getAbsolutePath(),
				"subject=" + subject });
		System.out.println(FileUtils.readFileToString(outfile).trim());
	}

}
