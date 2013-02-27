/* Copyright 2013 Fabian Steeg, Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.util.ArrayList;
import java.util.List;

import org.culturegraph.metastream.annotation.Description;
import org.culturegraph.metastream.annotation.In;
import org.culturegraph.metastream.annotation.Out;
import org.culturegraph.metastream.framework.StreamReceiver;

/**
 * @author Fabian Steeg
 */
@Description("Encode a stream as Graphviz DOT")
@In(StreamReceiver.class)
@Out(String.class)
public final class PipeEncodeDot extends AbstractGraphPipeEncoder {

	private List<String> predicates;
	private List<String> objects;

	@Override
	protected void onSetReceiver() {
		super.onSetReceiver();
		getReceiver().process("digraph g {");
		getReceiver().process("\tgraph[layout=fdp]");
	}

	@Override
	protected void onCloseStream() {
		getReceiver().process("}");
		super.onCloseStream();
	}

	@Override
	public void startRecord(final String identifier) {
		this.subject = null;
		predicates = new ArrayList<>();
		objects = new ArrayList<>();
	}

	@Override
	public void literal(final String name, final String value) {
		if (name.equalsIgnoreCase(SUBJECT_NAME)) {
			this.subject = value;
		} else {
			predicates.add(name);
			objects.add(uriOrLiteral(value));
		}
	}

	@Override
	public void endRecord() {
		for (int i = 0; i < predicates.size(); i++) {
			String object = objects.get(i);
			object = object.charAt(0) == '"' ? object : "\"" + object + "\"";
			getReceiver().process(
					String.format("\t\"%s\" -> %s [label=\"%s\"]", subject,
							object, predicates.get(i)));
		}
	}
}
