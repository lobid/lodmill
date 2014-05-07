package org.lobid.lodmill;

import java.util.Iterator;
import java.util.Set;

import com.hp.hpl.jena.graph.Factory;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleBoundary;
import com.hp.hpl.jena.util.CollectionFactory;

/**
 * GraphExtract offers a very simple recursive extraction of a subgraph with a
 * specified root in some supergraph. The recursion is terminated by triples
 * that satisfy some supplied boundary condition.
 */
public class GraphExtract {
	protected final TripleBoundary b;

	public GraphExtract(TripleBoundary b) {
		this.b = b;
	}

	/**
	 * Answer a new graph which is the reachable subgraph from <code>node</code>
	 * in <code>graph</code> with the terminating condition given by the
	 * TripleBoundary passed to the constructor.
	 */
	public Graph extract(Node node, Graph graph) {
		return extractInto(Factory.createGraphMem(), node, graph);
	}

	/**
	 * Answer the graph <code>toUpdate</code> augmented with the sub-graph of
	 * <code>extractFrom</code> reachable from <code>root</code> bounded by this
	 * instance's TripleBoundary.
	 */
	public Graph extractInto(Graph toUpdate, Node root, Graph extractFrom) {
		new Extraction(b, toUpdate, extractFrom).extractInto(root);
		return toUpdate;
	}

	/**
	 * This is the class that does all the work, in the established context of the
	 * source and destination graphs, the TripleBoundary that determines the
	 * limits of the extraction, and a local set <code>active</code> of nodes
	 * already seen and hence not to be re-processed.
	 */
	protected static class Extraction {
		protected Graph toUpdate;
		protected Graph extractFrom;
		protected Set<Node> active;
		protected TripleBoundary b;

		Extraction(TripleBoundary b, Graph toUpdate, Graph extractFrom) {
			this.toUpdate = toUpdate;
			this.extractFrom = extractFrom;
			this.active = CollectionFactory.createHashedSet();
			this.b = b;
		}

		public void extractInto(Node root) {
			active.add(root);
			Iterator<Triple> it = extractFrom.find(root, Node.ANY, Node.ANY);
			while (it.hasNext()) {
				Triple t = it.next();
				Node subRoot = t.getObject();
				if (!(b.stopAt(t))) {
					toUpdate.add(t);
					if (!(active.contains(subRoot)))
						extractInto(subRoot);
				}
			}
		}
	}
}
