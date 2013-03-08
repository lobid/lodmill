/* Copyright 2013 Fabian Steeg, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.util.ArrayList;
import java.util.List;

import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

/**
 * @author Fabian Steeg, Pascal Christoph
 */
@Description("Encode a stream as N-Triples")
@In(StreamReceiver.class)
@Out(String.class)
public final class PipeEncodeTriples extends AbstractGraphPipeEncoder {

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
			list.add(String.format("<%s> %s .", name, uriOrLiteral(value)));
		}
	}

	@Override
	public void endRecord() {
		for (String predicateObject : list) {
			getReceiver().process("<" + subject + "> " + predicateObject);
		}
	}

}
