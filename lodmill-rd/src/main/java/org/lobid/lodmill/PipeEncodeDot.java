/* Copyright 2013 Fabian Steeg, Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.util.ArrayList;
import java.util.List;

import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

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
		predicates = new ArrayList<String>();
		objects = new ArrayList<String>();
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
			object = object.charAt(0) == '"' ? object : "\"" + object + "\""; // NOPMD
			getReceiver().process(
					String.format("\t\"<%s>\" -> %s [label=\"%s\"]", subject, object,
							predicates.get(i)));
		}
	}

	private static String uriOrLiteral(final String value) {
		return isUriWithScheme(value) ? "<" + value + ">" : "\"" + value + "\"";
	}

}
