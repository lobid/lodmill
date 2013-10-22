/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package controllers;

import java.util.Arrays;
import java.util.List;

import org.lobid.lodmill.JsonLdConverter;

/**
 * Supported RDF serializations for content negotiation.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
/* no javadoc for elements */
public enum Serialization {/* @formatter:off */
		JSON_LD(null, Arrays.asList("application/json", "application/ld+json")),
		RDF_A(null, Arrays.asList("text/html", "text/xml", "application/xml")),
		RDF_XML(JsonLdConverter.Format.RDF_XML, Arrays.asList("application/rdf+xml")),
		N_TRIPLE(JsonLdConverter.Format.N_TRIPLE, Arrays.asList("text/plain")),
		N3(JsonLdConverter.Format.N3, Arrays.asList("text/rdf+n3", "text/n3")),
		TURTLE(JsonLdConverter.Format.TURTLE, /* @formatter:on */
			Arrays.asList("application/x-turtle", "text/turtle"));

	JsonLdConverter.Format format;
	List<String> types;

	/** @return The content types associated with this serialization. */
	public List<String> getTypes() {
		return types;
	}

	private Serialization(final JsonLdConverter.Format format,
			final List<String> types) {
		this.format = format;
		this.types = types;
	}
}