/* Copyright 2013 Pascal Christoph, hbz. Licensed under the Eclipse Public License 1.0 */

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

import com.hp.hpl.jena.rdf.model.LiteralRequiredException;
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
		+ "- 'stopIndex' ( a subfolder will be extracted out of the filename. This marks the index' end )")
@In(Model.class)
@Out(Void.class)
public final class RdfModelFileWriter extends DefaultObjectReceiver<Model> {
	private static final Logger LOG = LoggerFactory
			.getLogger(RdfModelFileWriter.class);
	private String target = "tmp";

	private String encoding = "UTF-8";
	private String property = "http://purl.org/dc/terms/identifier";
	private String fileSuffix = "nt";
	private int startIndex = 0;
	private int endIndex = 0;
	private Lang serialization = Lang.NTRIPLES;

	/**
	 * Default constructor
	 */
	public RdfModelFileWriter() {
	}

	/**
	 * Returns the encoding used to open the resource.
	 * 
	 * @return current default setting
	 */
	public String getEncoding() {
		return encoding;
	}

	/**
	 * Sets the encoding used to open the resource.
	 * 
	 * @param encoding new encoding
	 */
	public void setEncoding(final String encoding) {
		this.encoding = encoding;
	}

	/**
	 * Sets the target path.
	 * 
	 * @param target the basis directory in which the files are stored
	 */
	public void setTarget(final String target) {
		this.target = target;
	}

	/**
	 * Sets the property in the RDF model which will be used to create the file
	 * names main part. This should be a unique value because if the generated
	 * filename is already existing the file would be overwritten."
	 * 
	 * @param property the property in the RDF model. The object of this property
	 *          will be the main part of the file's name.
	 */
	public void setProperty(final String property) {
		this.property = property;
	}

	/**
	 * Sets the file's suffix.
	 * 
	 * @param fileSuffix the suffix used for the to be generated files
	 */
	public void setFileSuffix(final String fileSuffix) {
		this.fileSuffix = fileSuffix;
	}

	/**
	 * Sets the beginning of the index in the filename to extract the name of the
	 * subfolder.
	 * 
	 * @param startIndex This marks the index'beginning.
	 */
	public void setStartIndex(final int startIndex) {
		this.startIndex = startIndex;
	}

	/**
	 * Sets the end of the index in the filename to extract the name of the
	 * subfolder.
	 * 
	 * @param endIndex This marks the index' end.
	 */
	public void setEndIndex(final int endIndex) {
		this.endIndex = endIndex;
	}

	/**
	 * 
	 * @param serialization Sets the serialization format. Default is NTriples.
	 */
	public void setSerialization(final String serialization) {
		this.serialization = RDFLanguages.nameToLang(serialization);
	}

	@Override
	public void process(final Model model) {
		String identifier = null;
		try {
			identifier =
					model.listObjectsOfProperty(model.createProperty(this.property))
							.next().asLiteral().toString();
			LOG.debug("Going to store identifier=" + identifier);
		} catch (NoSuchElementException e) {
			LOG.warn(
					"No identifier => cannot derive a filename for " + model.toString(),
					e);
			return;
		} catch (LiteralRequiredException e) {
			LOG.warn("Identifier is a URI. Derive filename from that URI ... "
					+ model.toString(), e);
			identifier =
					model.listObjectsOfProperty(model.createProperty(this.property))
							.next().toString();
		}
		final String file =
				FilenameUtils.concat(
						target,
						FilenameUtils.concat(identifier.substring(startIndex, endIndex)
								+ File.separator, identifier + "." + this.fileSuffix));
		LOG.info("Write to " + file);
		ensurePathExists(file);

		try {
			final Writer writer =
					new OutputStreamWriter(new FileOutputStream(file), encoding);
			final StringWriter tripleWriter = new StringWriter();
			RDFDataMgr.write(tripleWriter, model, this.serialization);
			tripleWriter.toString();
			IOUtils.write(tripleWriter.toString(), writer);
			writer.close();
		} catch (IOException e) {
			throw new MetafactureException(e);
		}
	}

	private static void ensurePathExists(final String path) {
		final File parent = new File(path).getAbsoluteFile().getParentFile();
		parent.mkdirs();
	}

}
