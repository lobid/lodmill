/** Copyright 2013 hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 
 **/
package org.lobid.lodmill;

import java.util.HashSet;

import org.apache.commons.lang3.StringEscapeUtils;
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

	private String entity;
	private StringBuilder builder = new StringBuilder();
	private HashSet<String> namespaces = new HashSet<String>();
	private boolean inEntity = false;
	private int recordCnt = 0;
	private String root;

	/**
	 * Sets the name of the entity. All these entities in the XML stream will be
	 * XML documents on their own.
	 * 
	 * @param name Identifies the entities
	 */
	public void setEntityName(final String name) {
		this.entity = name;
	}

	/**
	 * Sets the top-level XML document element.
	 * 
	 * @param name the element
	 */
	public void setTopLevelElement(final String name) {
		this.root = name;
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
			if (entity.equals(localName)) {
				builder = new StringBuilder();
				getReceiver().startRecord(String.valueOf(this.recordCnt++));
				inEntity = true;
				appendValuesToEntity(qName, attributes);
			} else if (this.root == null)
				this.root = qName;
		} else
			appendValuesToEntity(qName, attributes);
	}

	private void appendValuesToEntity(final String qName,
			final Attributes attributes) {
		this.builder.append("<" + qName);
		if (attributes.getLength() > 0) {
			for (int i = 0; i < attributes.getLength(); i++) {
				builder.append(" " + attributes.getQName(i) + "=\""
						+ StringEscapeUtils.escapeXml(attributes.getValue(i)) + "\"");
			}
		}
		builder.append(">");
	}

	@Override
	public void endElement(final String uri, final String localName,
			final String qName) throws SAXException {
		if (inEntity) {
			builder.append("</" + qName + ">");
			if (entity.equals(localName)) {
				StringBuilder sb = new StringBuilder("<" + this.root);
				if (namespaces != null) {
					for (String ns : namespaces) {
						sb.append(ns);
					}
					sb.append(">");
				}
				builder.insert(0, sb.toString()).append("</" + this.root + ">");
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
		builder.append(StringEscapeUtils
				.escapeXml(new String(chars, start, length)));
	}
}
