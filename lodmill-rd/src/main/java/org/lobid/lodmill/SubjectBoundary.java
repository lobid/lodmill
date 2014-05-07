/* Copyright 2014 Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleBoundary;

/**
 * A TripleBoundary where only the given subject is considered to be true. Also
 * excludes triples having blank nodes as objects.
 */
public final class SubjectBoundary implements TripleBoundary {
	Node subject;

	@Override
	public boolean stopAt(Triple t) {
		return (!subject.equals(t.getSubject()) || t.getObject().isBlank());
	}

	/**
	 * Sets the subject which is the boundary.
	 * 
	 * @param subject the boundary
	 */
	public final void setSubjectAsBoundary(Node subject) {
		this.subject = subject;
	}
}