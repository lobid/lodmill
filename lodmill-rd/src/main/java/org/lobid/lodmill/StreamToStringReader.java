/* Copyright 2013 Jan Schnasse.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.IOException;
import java.io.Reader;

import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

import com.google.common.io.CharStreams;

/**
 * Reads all content of Reader to one single string.
 * 
 * @author Jan Schnasse
 * 
 */
@Description("Reads all content of Reader to one single string.")
@In(Reader.class)
@Out(String.class)
public class StreamToStringReader extends
		DefaultObjectPipe<Reader, ObjectReceiver<String>> {

	@Override
	public void process(final Reader reader) {
		assert null != reader;
		process(reader, getReceiver());
	}

	private static void process(final Reader reader,
			final ObjectReceiver<String> receiver) {
		try {
			receiver.process(CharStreams.toString(reader));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
