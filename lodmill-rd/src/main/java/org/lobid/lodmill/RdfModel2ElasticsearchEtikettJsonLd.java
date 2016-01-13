/* Copyright 2013-2015 Fabian Steeg, Pascal Christoph, hbz. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.jena.riot.RDFDataMgr;
import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import de.hbz.lobid.helper.Globals;
import de.hbz.lobid.helper.JsonConverter;

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
public final class RdfModel2ElasticsearchEtikettJsonLd
		extends DefaultObjectPipe<Model, ObjectReceiver<HashMap<String, String>>> {
	private static final Logger LOG =
			LoggerFactory.getLogger(RdfModel2ElasticsearchEtikettJsonLd.class);
	// the items will have their own index type and ES parents
	private static final String PROPERTY_TO_PARENT = "exemplarOf";
	private static String LOBID_DOMAIN = "http://lobid.org/";
	private static String LOBID_ITEM_URI_PREFIX = LOBID_DOMAIN + "item/";
	// the sub node we want to cling to the main node
	private static final String KEEP_NODE_PREFIX = "http://d-nb.info/gnd";
	private static final String KEEP_NODE_MAIN_PREFIX =
			LOBID_DOMAIN + "resource/";
	private static String mainNodeId;
	private static final String TYPE_ITEM = "json-ld-lobid-item";
	private static final String TYPE_RESOURCE = "json-ld-lobid";

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
			Model submodel = ModelFactory.createDefaultModel();
			if (!subjectResource.isAnon()) {
				// only extract sub nodes we don't want to keep in the main model
				if (!subjectResource.getURI().startsWith(KEEP_NODE_PREFIX)
						&& !subjectResource.getURI().startsWith(KEEP_NODE_MAIN_PREFIX)) {
					if (shouldSubmodelBeExtracted(submodel, subjectResource)) {
						toJson(submodel, subjectResource.getURI().toString());
					}
				} else if (subjectResource.getURI().toString().startsWith(LOBID_DOMAIN))
					mainNodeId = subjectResource.getURI().toString();
			}
			if (!submodel.isEmpty()) {
				// remove the newly created sub model from the main node
				copyOfOriginalModel.remove(submodel);
			}
		}
		// the main node (with its kept sub node)
		toJson(copyOfOriginalModel, mainNodeId);
	}

	// A sub model mustn't be extracted if the resource is to be kept as a sub
	// node of the main node. An bnode mustn't be extracted either.
	private static boolean shouldSubmodelBeExtracted(Model submodel,
			Resource subjectResource) {
		StmtIterator stmtIt = subjectResource.listProperties();
		while (stmtIt.hasNext()) {
			Statement stmt = stmtIt.nextStatement();
			// identifying the main node
			if (stmt.getObject().toString().startsWith(KEEP_NODE_PREFIX))
				return false;
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
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			JsonConverter jc = new JsonConverter();
			RDFDataMgr.write(out, model, org.apache.jena.riot.RDFFormat.NTRIPLES);
			Map<String, Object> jsonMap =
					jc.convert(new ByteArrayInputStream(out.toByteArray()),
							org.openrdf.rio.RDFFormat.NTRIPLES, "http://lobid.org",
							Globals.etikette.getContext().get("@context"));
			getReceiver().process(addInternalProperties(new HashMap<String, String>(),
					id, JsonConverter.getObjectMapper().writeValueAsString(jsonMap)));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static HashMap<String, String> addInternalProperties(
			HashMap<String, String> jsonMap, String id, String json) {
		String internal_parent = "";
		String type = TYPE_RESOURCE;
		if (id.startsWith(LOBID_ITEM_URI_PREFIX)) {
			type = TYPE_ITEM;
			try {
				JsonNode node = new ObjectMapper().readValue(json, JsonNode.class);
				final JsonNode parent = node.findValue(PROPERTY_TO_PARENT);
				String p = parent != null ? parent.findValue("@id").asText() : null;
				internal_parent = ",\"_parent\":\"" + p + "\"";
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
		String jsonDocument = json + internal_parent;
		jsonMap.put(ElasticsearchIndexer.Properties.GRAPH.getName(), jsonDocument);
		jsonMap.put(ElasticsearchIndexer.Properties.TYPE.getName(), type);
		jsonMap.put(ElasticsearchIndexer.Properties.ID.getName(), id);
		return jsonMap;
	}

}
