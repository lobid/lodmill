/* Copyright 2013 Jan Schnasse.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.StringReader;

import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * Decodes lines of ntriples
 * 
 * @author Jan Schnasse
 * 
 */
@Description("Decodes a record in ntriple format. "
		+ "Creates a new entity for each statement. "
		+ "The rdf subject is decoded as entity, "
		+ "rdf predicates and rdf objects are decoded as literals (as key-value pairs).")
@In(String.class)
@Out(StreamReceiver.class)
public final class NTripleDecoder extends
		DefaultObjectPipe<String, StreamReceiver> {
	private int count = 0;

	@Override
	public void process(final String str) {
		getReceiver().startRecord(String.valueOf(++count));
		Model model = ModelFactory.createDefaultModel();
		model.read(new StringReader(str), "test:uri:" + count, "N-TRIPLE");
		StmtIterator iterator = model.listStatements();
		while (iterator.hasNext()) {
			Statement stm = iterator.next();
			Resource subject = stm.getSubject();
			Property predicate = stm.getPredicate();
			RDFNode object = stm.getObject();
			getReceiver().startEntity(subject.toString());
			if (object.isLiteral()) {
				getReceiver().literal(predicate.toString(),
						object.asLiteral().getString());
			} else {
				getReceiver().literal(predicate.toString(), object.toString());
			}
			getReceiver().endEntity();
		}
		getReceiver().endRecord();
	}
}
