/* Copyright 2013 Fabian Steeg, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.net.URI;
import java.net.URISyntaxException;

import org.culturegraph.mf.framework.DefaultStreamPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

/**
 * @author Fabian Steeg, Pascal Christoph
 */
@Description("Superclass for graph-based pipe encoders")
@In(StreamReceiver.class)
@Out(String.class)
public abstract class AbstractGraphPipeEncoder extends
		DefaultStreamPipe<ObjectReceiver<String>> {

	static final String SUBJECT_NAME = "subject";
	String subject;

	String uriOrLiteral(final String value) {
		return isUriWithScheme(value) ? "<" + value + ">" : "\"" + value + "\"";
	}

	private boolean isUriWithScheme(final String value) {
		try {
			final URI uri = new URI(value);
			/*
			 * collection:example.org" is a valid URI, though no URL, and " 1483-1733"
			 * is also a valid (java-)URI, but not for us - a "scheme" is mandatory.
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
