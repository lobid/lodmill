/* Copyright 2013 Fabian Steeg, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.StringWriter;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.util.ResourceUtils;

/**
 * Treats Literals, URIs and Blank Nodes. The latter will be invoked by using
 * the <entity> element in the morph file. Output are N-Triples.
 * 
 * @author Fabian Steeg, Pascal Christoph
 */
@Description("Encode a stream as N-Triples")
@In(StreamReceiver.class)
@Out(String.class)
public class PipeEncodeTriples extends AbstractGraphPipeEncoder {
	Model model;
	Stack<Resource> resources;
	private final AtomicInteger ATOMIC_INT = new AtomicInteger();
	// dummy subject to store data even if the subject is unknown at first
	final static String DUMMY_SUBJECT = "dummy_subject";
	final static String HTTP = "http";
	final static String URN = "urn";
	private boolean fixSubject = false;
	private static final Logger LOG = LoggerFactory
			.getLogger(PipeEncodeTriples.class);
	private boolean storeUrnAsUri = false;

	/**
	 * Sets the default temporary subject.
	 */
	public PipeEncodeTriples() {
		subject = DUMMY_SUBJECT;
	}

	/**
	 * Allows to define the subject from outside, e.g. from a flux file.
	 * 
	 * @param subject set the subject for each triple
	 */
	public void setSubject(final String subject) {
		this.subject = subject;
		fixSubject = true;
	}

	/**
	 * Allows to store URN's as URI's- Default is to store them as literals.
	 * 
	 * @param storeUrnAsUri set if urn's should be stored as URIs
	 */
	public void setStoreUrnAsUri(final String storeUrnAsUri) {
		this.storeUrnAsUri = Boolean.parseBoolean(storeUrnAsUri);
	}

	@Override
	public void startRecord(final String identifier) {
		model = ModelFactory.createDefaultModel();
		resources = new Stack<Resource>();
		if (!fixSubject) {
			subject = DUMMY_SUBJECT;
		}
		resources.push(model.createResource(subject));
	}

	@Override
	public void literal(final String name, final String value) {
		if (value == null)
			return;
		if (name.equalsIgnoreCase(SUBJECT_NAME)) {
			subject = value;
			try {
				if (resources.peek().hasURI((DUMMY_SUBJECT))) {
					ResourceUtils.renameResource(model.getResource(DUMMY_SUBJECT),
							subject);
					resources.push(model.createResource(subject));
				} else {
					resources.push(model.createResource(subject));
				}
			} catch (Exception e) {
				LOG.warn("Problem with name=" + name + " value=" + value, e);
			}
		} else if (name.startsWith(HTTP)) {
			try {
				final Property prop = model.createProperty(name);
				if (isUriWithScheme(value)
						&& ((value.startsWith(URN) && storeUrnAsUri)
								|| value.startsWith(HTTP) || value.startsWith("mailto"))) {
					resources.peek().addProperty(prop,
							model.asRDFNode(NodeFactory.createURI(value)));
				} else {
					resources.peek().addProperty(prop, value);
				}
			} catch (Exception e) {
				LOG.warn("Problem with name=" + name + " value=" + value, e);
			}
		}
	}

	Resource makeBnode(final String value) {
		final Resource res =
				model.createResource(new AnonId("_:" + value
						+ ATOMIC_INT.getAndIncrement()));
		model.add(resources.peek(), model.createProperty(value), res);
		return res;
	}

	void enterBnode(final Resource res) {
		this.resources.push(res);
	}

	@Override
	public void startEntity(final String name) {
		enterBnode(makeBnode(name));
	}

	@Override
	public void endEntity() {
		this.resources.pop();
	}

	@Override
	public void endRecord() {
		// insert subject now if it was not at the beginning of the record
		final StringWriter tripleWriter = new StringWriter();
		RDFDataMgr.write(tripleWriter, model, Lang.NTRIPLES);
		getReceiver().process(tripleWriter.toString());
	}
}
