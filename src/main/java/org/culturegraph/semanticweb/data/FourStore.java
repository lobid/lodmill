/* Copyright 2012 Fabian Steeg. Licensed under the Apache License Version 2.0 */

package org.culturegraph.semanticweb.data;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSetFormatter;

/**
 * Access and insert data in a 4store triple store. Most access is based on
 * SPARQL, so it should work with other triple stores as well. See comments for
 * 4store specific methods.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class FourStore {

	private static final Logger LOG = LoggerFactory.getLogger(FourStore.class);
	private final String root;

	/** @param root The store's root URL */
	public FourStore(final String root) {
		this.root = root;
	}

	/**
	 * @param query The CONSTRUCT query
	 * @return The resulting graph
	 */
	public Graph sparqlConstruct(final String query) {
		final QueryExecution qexec = createQueryExecution(query);
		try {
			return qexec.execConstruct().getGraph();
		} catch (Exception x) {
			LOG.error("Exception for query: " + query, x);
			return Graph.emptyGraph;
		} finally {
			qexec.close();
		}
	}

	/**
	 * @param query The SELECT query
	 * @return The query solutions
	 */
	public List<QuerySolution> sparqlSelect(final String query) {
		final QueryExecution qexec = createQueryExecution(query);
		try {
			return ResultSetFormatter.toList(qexec.execSelect());
		} catch (Exception x) {
			LOG.error("Exception for query: " + query, x);
			return Collections.emptyList();
		} finally {
			qexec.close();
		}
	}

	/**
	 * Delete a named Graph using 4store's specific deletion method <br/>
	 * NOTICE: this method does NOT use standard SPARQL
	 * 
	 * @param graph The graph to delete
	 * @return The server's response to the deletion request
	 * @throws IOException When the server communication fails
	 */
	public HttpResponse deleteGraph(final String graph) throws IOException {
		LOG.info("DELETE " + graph);
		return new DefaultHttpClient().execute(new HttpRequestBase() {
			public String getMethod() {
				return "DELETE";
			}

			public URI getURI() {
				return URI.create(root + "/data/" + graph);
			}
		});
	}

	/**
	 * Insert a triple into a named graph using SPARQL syntax. <br/>
	 * NOTICE: this method DOES use standard SPARQL, but sets the content type
	 * to "application/x-www-form-urlencoded" as required by 4store. This might
	 * work with other stores, but it's not what i.e. Jena does (see inline
	 * comment).
	 * 
	 * @param graph The named graph to insert the triple
	 * @param triple The triple as to be inserted into the graph
	 * @return The server's response to the insertion request
	 * @throws IOException When the server communication fails
	 */
	public HttpResponse insertTriple(final String graph, final Triple triple)
			throws IOException {
		return insertTriple(graph, String.format("<%s> <%s> <%s>", triple
				.getSubject().toString(), triple.getPredicate().toString(),
				triple.getObject().toString()));
	}

	private HttpResponse insertTriple(final String graph, final String triple)
			throws IOException {
		final String endpoint = root + "/update/";
		return post(endpoint,
				String.format("INSERT DATA { GRAPH <%s> {%s}} ", graph, triple));
	}

	private HttpResponse post(final String endpoint, final String sparql)
			throws IOException {
		/* Update via Jena API, but 4store req. x-www-form-urlencoded */
		// final UpdateRequest queryObj = UpdateFactory.create(sparql);
		// UpdateExecutionFactory.createRemote(queryObj, endpoint).execute();
		/* So instead, we use HttpClient directly: */
		final HttpPost post = new HttpPost(endpoint);
		final List<NameValuePair> params = new ArrayList<NameValuePair>();
		params.add(new BasicNameValuePair("update", sparql));
		post.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
		post.setHeader("content-type", "application/x-www-form-urlencoded");
		return new DefaultHttpClient().execute(post);
	}

	private QueryExecution createQueryExecution(final String query) {
		LOG.debug("Query for: " + query);
		final QueryExecution qexec = QueryExecutionFactory.sparqlService(root
				+ "/sparql/", QueryFactory.create(query));
		return qexec;
	}
}
