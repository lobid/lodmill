/* Copyright 2013 Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.StringReader;

import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Encodes triples into a jena RDF Model.
 * 
 * @author Pascal Christoph
 */
@Description("Encodes triples into an RDF Model. Predefined values for input are"
		+ " 'RDF/XML', 'N-TRIPLE', 'TURTLE' (or 'TTL') and 'N3'. null represents the "
		+ "default language, 'RDF/XML'. 'RDF/XML-ABBREV' is a synonym for 'RDF/XML'."
		+ "Default is 'TURTLE'.")
@In(String.class)
@Out(Model.class)
public class Triples2RdfModel extends
		DefaultObjectPipe<String, ObjectReceiver<Model>> {
	private int count = 0;
	private String serialization = "TURTLE";
	private static final Logger LOG = LoggerFactory
			.getLogger(Triples2RdfModel.class);

	/**
	 * Sets the serialization format of the incoming triples .
	 * 
	 * @param serialization one of 'RDF/XML', 'N-TRIPLE', 'TURTLE' (or 'TTL') and
	 *          'N3'. null represents the default language, 'RDF/XML'.
	 *          'RDF/XML-ABBREV' is a synonym for 'RDF/XML'.")
	 */
	public void setInput(final String serialization) {
		this.serialization = serialization;
	}

	@Override
	public void process(final String str) {
		Model model = ModelFactory.createDefaultModel();
		try {
			model.read(new StringReader(str), "test:uri:" + count++, serialization);
			getReceiver().process(model);
		} catch (Exception e) {
			LOG.error("Exception in " + str, e);
		}
	}
}
