/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */
package tests;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Scanner;

/**
 * Java sample usage for the Lobid API: get all results for a specified base
 * URL, paging through the result sets using the `from` parameter.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class SampleUsage {

	private static final int SIZE = 100;

	/**
	 * @param args The base URL, content type, and output file name, or nothing
	 *          (for default values). See api.lobid.org for URLs & content types.
	 * @throws IOException on connection problems
	 * @throws MalformedURLException on connection problems
	 * @throws InterruptedException on sleep problems
	 */
	public static void main(String[] args) throws MalformedURLException,
			IOException, InterruptedException {
		String defaultBase = "http://api.lobid.org/resource?set=NWBib";
		String defaultContent = "text/plain"; // N-Triples, see api.lobid.org
		String defaultFile = "NWBib.nt";
		checkArgs(args, defaultBase, defaultContent, defaultFile);
		String base = args.length == 3 ? args[0] : defaultBase;
		String content = args.length == 3 ? args[1] : defaultContent;
		String file = args.length == 3 ? args[2] : defaultFile;
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
			for (int from = 0; load(base, from, content, writer) > 0; from += SIZE)
				Thread.sleep(500); // give the server a little break
		}
	}

	private static void checkArgs(String[] args, String defaultBase,
			String defaultContent, String defaultFile) {
		if (args.length != 0 && args.length != 3) {
			System.err.println(String.format(
					"Pass 3 arguments or 0 for defaults: base URL (default: '%s'), "
							+ "content type (default: '%s'), output file (default '%s')",
					defaultBase, defaultContent, defaultFile));
			System.exit(-1);
		}
	}

	private static long load(String base, int from, String content, Writer writer)
			throws IOException, MalformedURLException {
		URLConnection connection =
				new URL(String.format("%s&from=%s&size=%s", base, from, SIZE))
						.openConnection();
		connection.setRequestProperty("Accept", content);
		System.out.println("GET " + connection.getURL());
		try (Scanner scanner =
				new Scanner(new BufferedInputStream(connection.getInputStream()))) {
			while (scanner.hasNextLine())
				writer.write(String.format("%s%n", scanner.nextLine()));
		}
		return connection.getContentLengthLong();
	}

}
