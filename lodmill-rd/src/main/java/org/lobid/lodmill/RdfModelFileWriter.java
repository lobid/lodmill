/* Copyright 2013 Pascal Christoph, hbz.
 * Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.NoSuchElementException;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.culturegraph.mf.exceptions.MetafactureException;
import org.culturegraph.mf.framework.DefaultObjectReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;

/**
 * A sink, writing triples into files. The filenames are constructed from the
 * literal of an given property.
 * 
 * @author Pascal Christoph
 */
@Description("Writes the object value of an RDF model into a file. Default serialization is 'NTRIPLES'. The filename is "
		+ "constructed from the literal of an given property (recommended properties are identifier)."
		+ " Variable are "
		+ "- 'target' (determining the output directory)"
		+ "- 'property' (the property in the RDF model. The object of this property"
		+ " will be the main part of the file's name.) "
		+ "- 'startIndex' ( a subfolder will be extracted out of the filename. This marks the index' beginning )"
		+ "- 'stopIndex' ( a subfolder will be extracted out of the filename. This marks the index' end )"
		+ "- 'serialization (e.g. one of 'NTRIPLES', 'TURTLE', 'RDFXML','RDFJSON'")
@In(Model.class)
@Out(Void.class)
public final class RdfModelFileWriter extends DefaultObjectReceiver<Model>
		implements FilenameExtractor, RDFSink {
	private static final Logger LOG = LoggerFactory
			.getLogger(RdfModelFileWriter.class);

	private FilenameUtil filenameUtil = new FilenameUtil();
	private Lang serialization;

	/**
	 * Default constructor
	 */
	public RdfModelFileWriter() {
		setProperty("http://purl.org/dc/terms/identifier");
		setFileSuffix("nt");
		setSerialization("NTRIPLES");
	}

	@Override
	public String getEncoding() {
		return filenameUtil.encoding;
	}

	@Override
	public void setEncoding(final String encoding) {
		filenameUtil.encoding = encoding;
	}

	@Override
	public void setTarget(final String target) {
		filenameUtil.target = target;
	}

	@Override
	public void setProperty(final String property) {
		filenameUtil.property = property;
	}

	@Override
	public void setFileSuffix(final String fileSuffix) {
		filenameUtil.fileSuffix = fileSuffix;
	}

	@Override
	public void setStartIndex(final int startIndex) {
		filenameUtil.startIndex = startIndex;
	}

	@Override
	public void setEndIndex(final int endIndex) {
		filenameUtil.endIndex = endIndex;
	}

	@Override
	public void setSerialization(final String serialization) {
		this.serialization = RDFLanguages.nameToLang(serialization);
	}

	@Override
	public void process(final Model model) {
		String identifier = null;
		try {
			identifier =
					model
							.listObjectsOfProperty(
									model.createProperty(filenameUtil.property)).next()
							.toString();
			LOG.debug("Going to store identifier=" + identifier);
		} catch (NoSuchElementException e) {
			LOG.warn("No identifier => cannot derive a filename for "
					+ model.toString());
			return;
		}

		String directory = identifier;
		if (directory.length() >= filenameUtil.endIndex) {
			directory =
					directory.substring(filenameUtil.startIndex, filenameUtil.endIndex);
		}
		final String file =
				FilenameUtils.concat(
						filenameUtil.target,
						FilenameUtils.concat(directory + File.separator, identifier + "."
								+ filenameUtil.fileSuffix));
		LOG.debug("Write to " + file);
		ensurePathExists(file);

		try {
			final Writer writer =
					new OutputStreamWriter(new FileOutputStream(file),
							filenameUtil.encoding);
			final StringWriter tripleWriter = new StringWriter();
			RDFDataMgr.write(tripleWriter, model, this.serialization);
			IOUtils.write(tripleWriter.toString(), writer);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new MetafactureException(e);
		}
	}

	private static void ensurePathExists(final String path) {
		final File parent = new File(path).getAbsoluteFile().getParentFile();
		parent.mkdirs();
	}

}
