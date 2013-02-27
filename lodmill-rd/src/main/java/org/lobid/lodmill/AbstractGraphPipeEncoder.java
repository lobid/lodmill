/* Copyright 2013 Fabian Steeg, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.net.URI;
import java.net.URISyntaxException;

import org.culturegraph.metastream.annotation.Description;
import org.culturegraph.metastream.annotation.In;
import org.culturegraph.metastream.annotation.Out;
import org.culturegraph.metastream.converter.Encoder;
import org.culturegraph.metastream.framework.DefaultStreamPipe;
import org.culturegraph.metastream.framework.ObjectReceiver;
import org.culturegraph.metastream.framework.StreamReceiver;

/**
 * @author Fabian Steeg, Pascal Christoph
 */
@Description("Superclass for graph-based pipe encoders")
@In(StreamReceiver.class)
@Out(String.class)
public abstract class AbstractGraphPipeEncoder extends
		DefaultStreamPipe<ObjectReceiver<String>> implements Encoder {

	static final String SUBJECT_NAME = "subject";
	String subject;

	String uriOrLiteral(final String value) {
		return isUriWithScheme(value) ? "<" + value + ">" : "\"" + value + "\"";
	}

	private boolean isUriWithScheme(final String value) {
		try {
			final URI uri = new URI(value);
			/*
			 * collection:example.org" is a valid URI, though no URL, and
			 * " 1483-1733" is also a valid (java-)URI, but not for us - a
			 * "scheme" is mandatory.
			 */
			if (uri.getScheme() == null) {
				return false;
			}
		} catch (URISyntaxException e) {
			return false;
		}
		return true;
	}
}
