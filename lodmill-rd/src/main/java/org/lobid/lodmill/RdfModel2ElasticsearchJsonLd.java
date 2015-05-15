/* Copyright 2013-2015 Fabian Steeg, Pascal Christoph, hbz. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.IOException;
import java.util.HashMap;

import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.jena.JenaRDFParser;
import com.github.jsonldjava.utils.JSONUtils;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * Converts a jena model to JSON-LD document(s) consumable by elasticsearch.
 * Every node in the graph will be a document on its own, except when this is
 * declared otherwise (-> keep node).
 * 
 * @author Fabian Steeg (fsteeg)
 * @author Pascal Christoph (dr0i)
 */
@In(Model.class)
@Out(HashMap.class)
public final class RdfModel2ElasticsearchJsonLd extends
		DefaultObjectPipe<Model, ObjectReceiver<HashMap<String, String>>> {
	private static final Logger LOG = LoggerFactory
			.getLogger(RdfModel2ElasticsearchJsonLd.class);
	// the items will have their own index type and ES parents
	private static final String PROPERTY_TO_PARENT =
			"http://purl.org/vocab/frbr/core#exemplarOf";
	private static String LOBID_ITEM_URI_PREFIX = "http://lobid.org/item/";
	// the sub node we want to cling to the main node
	private static final String KEEP_NODE_PREFIX = "http://d-nb.info/gnd";
	private static String mainNodeId;
	private static final String TYPE_ITEM = "json-ld-lobid-item";
	private static final String TYPE_RESOURCE = "json-ld-lobid";

	// stores index properties as well as the document itself
	private static HashMap<String, String> jsonMap;

	@Override
	public void process(final Model originModel) {
		splitModel2ItemAndResourceModel(originModel);
	}

	private void splitModel2ItemAndResourceModel(final Model originalModel) {
		Model copyOfOriginalModel =
				ModelFactory.createModelForGraph(originalModel.getGraph());
		final ResIterator subjectsIterator = originalModel.listSubjects();
		// iterate through all nodes
		while (subjectsIterator.hasNext()) {
			final Resource subjectResource = subjectsIterator.next();
			// just extract sub nodes we don't want to keep in the main model
			if (!subjectResource.getURI().startsWith(KEEP_NODE_PREFIX)) {
				Model submodel = ModelFactory.createDefaultModel();
				if (shouldSubmodelBeExtracted(submodel, subjectResource)) {
					toJson(submodel, subjectResource.getURI().toString());
					// remove the newly created sub model from the main node
					copyOfOriginalModel.remove(submodel);
				}
			}
		}
		// the main node (with its kept sub node)
		toJson(copyOfOriginalModel, mainNodeId);
	}

	// A sub model mustn't be extracted if the resource is to be kept as a sub
	// node of the main node. An bnode mustn't be extracted either.
	private static boolean shouldSubmodelBeExtracted(Model submodel,
			Resource subjectResource) {
		if (subjectResource.isAnon())
			return false;
		StmtIterator stmtIt = subjectResource.listProperties();
		while (stmtIt.hasNext()) {
			Statement stmt = stmtIt.nextStatement();
			// identifying the main node
			if (stmt.getObject().toString().startsWith(KEEP_NODE_PREFIX)) {
				mainNodeId = subjectResource.getURI().toString();
				return false;
			}
			submodel.add(stmt);
		}
		return true;
	}

	/**
	 * Creates and pushes two documents: the json document with the index
	 * properties and the json document itself. The 'expanded' JSON-LD
	 * serialization is used to guarantee consistent field types.
	 * 
	 * @param model
	 * @param id
	 */
	private void toJson(Model model, String id) {
		if (model.isEmpty())
			return;
		jsonMap = new HashMap<>();
		final JenaRDFParser parser = new JenaRDFParser();
		try {
			Object json = JsonLdProcessor.fromRDF(model, new JsonLdOptions(), parser);
			// the json document itself
			json = JsonLdProcessor.expand(json);
			// wrap json into a "@graph" for elasticsearch (still valid JSON-LD)
			String jsonDocument =
					"{\"@graph\":" + JSONUtils.toString(json) + ",\"internal_id\":\""
							+ id + "\"}";
			jsonMap
					.put(ElasticsearchIndexer.Properties.GRAPH.getName(), jsonDocument);
			// defining the elasticsearch index properties
			jsonMap = addInternalProperties(jsonMap, id, jsonDocument);
			getReceiver().process(jsonMap);
		} catch (JsonLdError e) {
			e.printStackTrace();
		}
	}

	private static HashMap<String, String> addInternalProperties(
			HashMap<String, String> jsonMap, String id, String jsonString) {
		String type = TYPE_RESOURCE;
		if (id.startsWith(LOBID_ITEM_URI_PREFIX)) {
			type = TYPE_ITEM;
			try {
				JsonNode node =
						new ObjectMapper().readValue(jsonString, JsonNode.class);
				final JsonNode parent = node.findValue(PROPERTY_TO_PARENT);
				String p = parent != null ? parent.findValue("@id").asText() : null;
				if (p == null) {
					LOG.warn("Item URI " + id + " has no parent declared!");
					jsonMap.put(ElasticsearchIndexer.Properties.PARENT.getName(),
							"no_parent");
				} else
					jsonMap.put(ElasticsearchIndexer.Properties.PARENT.getName(), p);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		jsonMap.put(ElasticsearchIndexer.Properties.TYPE.getName(), type);
		jsonMap.put(ElasticsearchIndexer.Properties.ID.getName(), id);
		return jsonMap;
	}

}
