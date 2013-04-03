/* Copyright 2013 Fabian Steeg, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.util.HashSet;
import java.util.Set;

import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

/**
 * Is aware of Literals, URIs and Blank Nodes . If the literal "name" equals
 * "bnode" it is assumed that the value of this literal is a triple where the
 * three entries are discriminated by a blank. Example:
 * 
 * <data source="032P.a" name="bnode"> <regexp match="(.*)"
 * format="_:a http://www.w3.org/2006/vcard/ns#street-address ${1}"/> </data>
 * 
 * The first entry is the blank node.
 * 
 * @author Fabian Steeg, Pascal Christoph
 */
@Description("Encode a stream as N-Triples")
@In(StreamReceiver.class)
@Out(String.class)
public final class PipeEncodeTriples extends AbstractGraphPipeEncoder {

	private Set<String> set;

	@Override
	public void startRecord(final String identifier) {
		this.subject = null;
		set = new HashSet<>(); // ensures no duplicates
		atomicInt.getAndIncrement();
	}

	@Override
	public void literal(final String name, String value) {
		try {
			value = new String(value.getBytes("UTF-8"), "UTF-8");
			if (name.equalsIgnoreCase(SUBJECT_NAME)) {
				this.subject = value;
			} else {
				if (name.equalsIgnoreCase(BNODE_NAME)) {
					set.add(String.format(
							"%s %s %s .",
							value.substring(0, value.indexOf(' ')) + atomicInt.get(),
							uriOrLiteralorBnode(value.substring(value.indexOf(' ') + 1,
									value.indexOf(' ', value.indexOf(' ') + 1))),
							uriOrLiteralorBnode(value.substring(value.indexOf(' ',
									value.indexOf(' ') + 1) + 1))));
				} else {
					set.add(String.format("<" + subject + "> <%s> %s .", name,
							uriOrLiteralorBnode(value)));
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void endRecord() {
		for (String predicateObject : set) {
			getReceiver().process(predicateObject);
		}
	}

}
