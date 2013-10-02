/** Copyright 2013 hbz
 * Licensed under the Eclipse Public License 1.0 
 **/

package org.lobid.lodmill;

import java.util.HashSet;

import org.culturegraph.mf.framework.DefaultXmlPipe;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.XmlReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/**
 * An XML entity splitter.
 * 
 * @author Pascal Christoph
 * 
 */
@Description("Splits all entities (aka records) residing in one XML document into multiple single XML documents.")
@In(XmlReceiver.class)
@Out(StreamReceiver.class)
public final class XmlEntitySplitter extends DefaultXmlPipe<StreamReceiver> {

	private String ENTITY;
	private StringBuilder builder = new StringBuilder();
	private HashSet<String> namespaces = new HashSet<String>();
	private boolean inEntity = false;
	private int recordCnt = 0;

	/**
	 * Sets the name of the entity. All these entities in the XML stream will be
	 * XML documents on their own.
	 * 
	 * @param name Identifies the entities
	 */
	public void setEntityName(final String name) {
		this.ENTITY = name;
	}

	@Override
	public void startPrefixMapping(String prefix, String uri) throws SAXException {
		super.startPrefixMapping(prefix, uri);
		if (!prefix.isEmpty() && uri != null)
			namespaces.add(" xmlns:" + prefix + "=\"" + uri + "\"");
	}

	@Override
	public void startElement(final String uri, final String localName,
			final String qName, final Attributes attributes) throws SAXException {
		if (!inEntity) {
			if (ENTITY.equals(localName)) {
				builder = new StringBuilder();
				getReceiver().startRecord(String.valueOf(this.recordCnt++));
				inEntity = true;
				appendValuesToEntity(qName, attributes);
			}
		} else
			appendValuesToEntity(qName, attributes);
	}

	private void appendValuesToEntity(final String qName,
			final Attributes attributes) {
		this.builder.append("<" + qName);
		if (attributes.getLength() > 0) {
			for (int i = 0; i < attributes.getLength(); i++) {
				builder.append(" " + attributes.getQName(i) + "=\""
						+ attributes.getValue(i) + "\"");
			}
		}
		builder.append(">");
	}

	@Override
	public void endElement(final String uri, final String localName,
			final String qName) throws SAXException {
		if (inEntity) {
			builder.append("</" + qName + ">");
			if (ENTITY.equals(localName)) {
				StringBuilder sb = new StringBuilder("<rdf:RDF");
				if (namespaces != null) {
					for (String ns : namespaces) {
						sb.append(ns);
					}
					sb.append(">");
				}
				builder.insert(0, sb.toString()).append("</rdf:RDF>");
				getReceiver().literal("entity", builder.toString());
				getReceiver().endRecord();
				inEntity = false;
				builder = new StringBuilder();
			}
		}
	}

	@Override
	public void characters(final char[] chars, final int start, final int length)
			throws SAXException {
		// remove tag indicators from values
		builder.append(new String(chars, start, length).replaceAll("<|>|&", " "));
	}
}
