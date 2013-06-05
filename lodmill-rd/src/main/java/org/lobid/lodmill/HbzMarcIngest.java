/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.morph.MorphErrorHandler;
import org.culturegraph.mf.stream.reader.MarcXmlReader;
import org.culturegraph.mf.stream.reader.Reader;
import org.culturegraph.mf.stream.sink.StringListMap;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ingest the hbz Marc21 export and log errors to debug issues with the format.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class HbzMarcIngest {

	private static final Logger LOG = LoggerFactory
			.getLogger(HbzMarcIngest.class);
	private static final String HBZ_MARC = "../../hbz.cg.20120725.mrc";
	private static final String REPORT_RAW = HBZ_MARC + "-report-raw.txt";
	private static final String REPORT_PROCESSED = HBZ_MARC
			+ "-report-processed.txt";
	private final Reader reader = new MarcXmlReader();
	private final Metamorph metamorph = new Metamorph(Thread.currentThread()
			.getContextClassLoader().getResourceAsStream("ingest.marc21.xml"));

	private final SortedSet<String> errorSet = new TreeSet<String>();
	private final Map<String, Integer> errorMap = new HashMap<String, Integer>();

	@SuppressWarnings("javadoc")
	@Test
	public void ingest() throws IOException {
		final StringListMap map = new StringListMap();
		reader.setReceiver(metamorph).setReceiver(map);
		final BufferedWriter rawReportWriter =
				new BufferedWriter(new FileWriter(REPORT_RAW));
		try {
			metamorph.setErrorHandler(new MorphErrorHandler() {
				@Override
				public void error(final Exception exception) {
					final String name = exception.getClass().getSimpleName();
					final String errorMessage =
							String.format("Metamorph error (%s): %s", name,
									exception.getMessage());
					processError(rawReportWriter, name, errorMessage);
				}
			});
			final BufferedReader scanner =
					new BufferedReader(new FileReader(HBZ_MARC));
			try {
				int all = 0;
				String line = null;
				while ((line = scanner.readLine()) != null) { // NOPMD (idiomatic usage)
					all++;
					try {
						reader.read(line);
					} catch (Exception e) {
						final String name = e.getClass().getSimpleName();
						final String errorMessage =
								String.format("Metastream error (%s): %s, record: %s", name,
										e.getMessage(), line);
						processError(rawReportWriter, name, errorMessage);
					}
				}
				Assert.assertTrue("Raw report file should exist",
						new File(REPORT_RAW).exists());
				writeProcessedReport(scanner, all);
			} finally {
				scanner.close();
			}
		} finally {
			rawReportWriter.close();
		}
		Assert.assertTrue("Processed report file should exist", new File(
				REPORT_PROCESSED).exists());
	}

	private void writeProcessedReport(final BufferedReader scanner, final int all)
			throws IOException {
		final BufferedWriter processedReportWriter =
				new BufferedWriter(new FileWriter(REPORT_PROCESSED));
		try {
			int err = 0;
			for (Integer i : errorMap.values()) {
				err += i;
			}
			final String summary =
					String.format("Processed %s records, got %s errors:", all, err);
			System.out.println(summary);
			processedReportWriter.write(summary + "\n");
			for (String s : errorMap.keySet()) {
				System.out.println(String.format("%s: %s", s, errorMap.get(s)));
				processedReportWriter.write(String.format("%s: %s\n", s,
						errorMap.get(s)));
			}
			scanner.close();
			for (String string : errorSet) {
				processedReportWriter.write(string + "\n");
			}
		} finally {
			processedReportWriter.close();
		}
	}

	private void processError(final BufferedWriter fullReportWriter,
			final String name, final String errorMEssage) {
		LOG.error(errorMEssage);
		errorSet.add(errorMEssage);
		errorMap.put(name,
				(errorMap.containsKey(name) ? errorMap.get(name) : 0) + 1);
		try {
			fullReportWriter.write(errorMEssage + "\n");
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
	}
}
