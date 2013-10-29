/* Copyright 2013 Pascal Christoph, hbz. 
 * Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.culturegraph.mf.exceptions.MetafactureException;
import org.culturegraph.mf.framework.DefaultStreamPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

/**
 * A sink, writing an xml file. The filename is constructed from the xpath given
 * via setProperty().
 * 
 * @author Pascal Christoph
 */
@Description("Writes the xml into the filesystem. The filename is constructed from the xpath given as 'property'."
		+ " Variables are "
		+ "- 'target' (determining the output directory)"
		+ "- 'property' (the element in the XML entity. Constitutes the main part of the file's name.) "
		+ "- 'startIndex' ( a subfolder will be extracted out of the filename. This marks the index' beginning )"
		+ "- 'stopIndex' ( a subfolder will be extracted out of the filename. This marks the index' end )")
@In(StreamReceiver.class)
@Out(Void.class)
public final class XmlFilenameWriter extends
		DefaultStreamPipe<ObjectReceiver<String>> implements
		ExtractFilenameInterface {
	private static final Logger LOG = LoggerFactory
			.getLogger(XmlFilenameWriter.class);
	private static final XPath xPath = XPathFactory.newInstance().newXPath();

	private ExtractFilename extractFilename = new ExtractFilename();

	/**
	 * Default constructor
	 */
	public XmlFilenameWriter() {
		setFileSuffix("xml");
	}

	@Override
	public void literal(final String str, String xml) {
		String identifier = null;
		try {
			identifier =
					xPath.evaluate(extractFilename.property, new InputSource(
							new StringReader(xml)));
		} catch (XPathExpressionException e2) {
			e2.printStackTrace();
		}
		if (identifier == null || identifier.length() < extractFilename.endIndex) {
			LOG.info("No identifier found, skip writing");
			LOG.debug("the xml:" + xml);
			return;
		}
		String directory = identifier;
		if (directory.length() >= extractFilename.endIndex) {
			directory =
					directory.substring(extractFilename.startIndex,
							extractFilename.endIndex);
		}
		final String file =
				FilenameUtils.concat(
						extractFilename.target,
						FilenameUtils.concat(directory + File.separator, identifier + "."
								+ extractFilename.fileSuffix));
		LOG.info("Write to " + file);
		extractFilename.ensurePathExists(file);
		try {
			final Writer writer =
					new OutputStreamWriter(new FileOutputStream(file),
							extractFilename.encoding);
			IOUtils.write(xml, writer);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new MetafactureException(e);
		}
	}

	@Override
	public String getEncoding() {
		return extractFilename.encoding;
	}

	@Override
	public void setEncoding(String encoding) {
		extractFilename.encoding = encoding;

	}

	@Override
	public void setTarget(String target) {
		extractFilename.target = target;
	}

	@Override
	public void setProperty(String property) {
		extractFilename.property = property;
	}

	@Override
	public void setFileSuffix(String fileSuffix) {
		extractFilename.fileSuffix = fileSuffix;

	}

	@Override
	public void setStartIndex(int startIndex) {
		extractFilename.startIndex = startIndex;

	}

	@Override
	public void setEndIndex(int endIndex) {
		extractFilename.endIndex = endIndex;
	}
}
