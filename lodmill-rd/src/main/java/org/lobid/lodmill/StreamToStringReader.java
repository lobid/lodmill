/* Copyright 2013 Jan Schnasse.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

import org.culturegraph.mf.exceptions.MetafactureException;
import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.ObjectReceiver;

/**
 * Reads all content of Reader to one single string.
 * 
 * @author Jan Schnasse
 * 
 */
public class StreamToStringReader extends
		DefaultObjectPipe<Reader, ObjectReceiver<String>> {
	private static final int BUFFER_SIZE = 1024 * 1024 * 16;

	@Override
	public void process(final Reader reader) {

		assert null != reader;
		process(reader, getReceiver());
	}

	private static void process(final Reader reader,
			final ObjectReceiver<String> receiver) {

		final BufferedReader buf = new BufferedReader(reader, BUFFER_SIZE);
		try {
			StringBuilder builder = new StringBuilder();
			String line = "";

			while ((line = buf.readLine()) != null) {
				builder.append(line);
			}

			receiver.process(builder.toString());

		} catch (IOException e) {
			throw new MetafactureException(e);
		}
	}
}
