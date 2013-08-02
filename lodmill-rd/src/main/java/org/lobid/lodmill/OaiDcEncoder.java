/* Copyright 2013 Jan Schnasse.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.StringWriter;

import org.apache.commons.lang3.StringEscapeUtils;
import org.culturegraph.mf.framework.DefaultStreamPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

/**
 * The XML Encoder takes the result of a morph an treats all literal names as
 * tag names and all literal values as tag values. The start and end of the
 * result can be modified using the setXmlPraeambel and setXmlPostambel methods.
 * 
 * @author Jan Schnasse
 * 
 */
@Description("Encodes streams in oai_dc xml. Takes the result of a morph "
		+ "an treats all literal names as tag names and all literal values as "
		+ "tag values.")
@In(StreamReceiver.class)
@Out(String.class)
public final class OaiDcEncoder extends
		DefaultStreamPipe<ObjectReceiver<String>> {
	private final StringWriter writer = new StringWriter();
	private final String xmlPraeambel =
			"<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
					+ "<oai_dc:dc \n"
					+ "\txmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\" \n"
					+ "\txmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
					+ "\txmlns:dcterms=\"http://purl.org/dc/terms/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" \n"
					+ "\txsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd\">\n";
	private final String xmlPostambel = "</oai_dc:dc>\n";

	@Override
	public void startRecord(final String id) {
		final StringBuffer buffer = writer.getBuffer();
		buffer.delete(0, buffer.length());
		writer.write(xmlPraeambel);
	}

	@Override
	public void endRecord() {
		writer.write(xmlPostambel);
		String result = writer.toString();
		if (getReceiver() != null)
			getReceiver().process(result);
	}

	@Override
	public void literal(final String name, final String value) {
		writer.write("<" + name + ">\n\t");
		writer.write(StringEscapeUtils.escapeXml(value));
		writer.write("\n</" + name + ">\n");
	}
}
