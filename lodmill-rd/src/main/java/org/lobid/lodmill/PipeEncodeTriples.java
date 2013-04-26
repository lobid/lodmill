/* Copyright 2013 Fabian Steeg, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicInteger;

import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFWriter;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.util.ResourceUtils;

/**
 * Is aware of Literals, URIs and Blank Nodes . If the literal "name" equals
 * "bnode" it is assumed that the value of this literal is a triple where the
 * three entries are discriminated by a blank. Example:
 * 
 * <data source="032P.a" name="bnode"> <regexp match="(.*)"
 * format="_:a http://www.w3.org/2006/vcard/ns#street-address ${1}"/> </data>
 * 
 * @TODO this workaround can maybe be substituted through use of <entity>, see
 *       http://lists.d-nb.de/pipermail/culturegraph/2013-April/000068.html
 * @author Fabian Steeg, Pascal Christoph
 */
@Description("Encode a stream as N-Triples")
@In(StreamReceiver.class)
@Out(String.class)
public class PipeEncodeTriples extends AbstractGraphPipeEncoder {
	Model model;
	Resource resource;
	static final String BNODE_NAME = "bnode";
	final AtomicInteger ATOMIC_INT = new AtomicInteger();

	@Override
	public void startRecord(final String identifier) {
		this.subject = "null";
		ATOMIC_INT.getAndIncrement();
		model = ModelFactory.createDefaultModel();
	}

	@Override
	public void literal(final String name, final String value) {
		if (name.equalsIgnoreCase(SUBJECT_NAME)) {
			this.subject = value;
		} else {
			if (name.equalsIgnoreCase(BNODE_NAME)) {
				processBnodeInObjectPosition(value);
			} else {
				resource = model.createResource(subject);
				final Property prop = model.createProperty(name);
				// create bnode in subject position
				if (value.startsWith("_:")) {
					resource.addProperty(
							prop,
							model.asRDFNode(Node.createAnon(new AnonId(value
									+ ATOMIC_INT.get()))));
				} else {
					if (isUriWithScheme(value)) {
						resource.addProperty(prop, model.asRDFNode(Node.createURI(value)));
					} else {
						resource.addProperty(prop, value);
					}
				}
			}
		}
	}

	private void processBnodeInObjectPosition(final String value) {
		final int indexOfFirstBlank = value.indexOf(' ');
		final int indexOfSecondBlank = value.indexOf(' ', indexOfFirstBlank + 1);
		resource =
				model.createResource(new AnonId(value.substring(0, indexOfFirstBlank)
						+ ATOMIC_INT.get()));
		final Property pro =
				model.createProperty(value.substring(indexOfFirstBlank + 1,
						indexOfSecondBlank));
		final String obj = value.substring(indexOfSecondBlank + 1);
		// check wether object is a URI or a literal
		if (isUriWithScheme(obj)) {
			resource.addProperty(pro, model.asRDFNode(Node.createURI(obj)));
		} else {
			resource.addProperty(pro, obj);
		}
	}

	@Override
	public void endRecord() {
		ResourceUtils.renameResource(model.getResource("null"), subject);
		final RDFWriter fasterWriter = model.getWriter("N-TRIPLES");
		final StringWriter tripleWriter = new StringWriter();
		fasterWriter.write(model, new PrintWriter(tripleWriter), null);
		getReceiver().process(tripleWriter.toString());
	}
}
