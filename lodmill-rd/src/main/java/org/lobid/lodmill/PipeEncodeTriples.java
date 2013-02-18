/* Copyright 2013 Fabian Steeg, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

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
@Description("Encode a stream as N-Triples")
@In(StreamReceiver.class)
@Out(String.class)
public final class PipeEncodeTriples extends
		DefaultStreamPipe<ObjectReceiver<String>> implements Encoder {

	private static final String SUBJECT_NAME = "subject";
	private String subject;
	private List<String> list;

	@Override
	public void startRecord(final String identifier) {
		this.subject = null;
		list = new ArrayList<>();
	}

	@Override
	public void literal(final String name, final String value) {
		if (name.equalsIgnoreCase(SUBJECT_NAME)) {
			this.subject = value;
		} else {
			final String object =
					isUriWithScheme(value) ? "<" + value + ">" : "\"" + value
							+ "\"";
			list.add(String.format("<%s> %s .", name, object));
		}
	}

	private boolean isUriWithScheme(final String value) {
		try {
			URI u = new URI(value);
			/*
			 * collection:example.org" is a valid URI, though no URL, and
			 * " 1483-1733" is also a valid (java-)URI, but not for us - a
			 * "scheme" is mandatory.
			 */
			if (u.getScheme() == null) {
				return false;
			}
		} catch (URISyntaxException e) {
			return false;
		}
		return true;
	}

	@Override
	public void endRecord() {
		for (String predicateObject : list) {
			getReceiver().process("<" + subject + "> " + predicateObject);
		}
	}

}
