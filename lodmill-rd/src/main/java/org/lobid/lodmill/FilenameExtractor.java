/* Copyright 2013 Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.File;

/**
 * Provides basic methods for constructing filenames out of entities.
 * 
 * @author Pascal Christoph (dr0i)
 * 
 */
public interface FilenameExtractor {

	/**
	 * Returns the encoding used to open the resource.
	 * 
	 * @return current default setting
	 */
	public String getEncoding();

	/**
	 * Sets the encoding used to open the resource.
	 * 
	 * @param encoding new encoding
	 */
	public void setEncoding(final String encoding);

	/**
	 * Sets the target path.
	 * 
	 * @param target the basis directory in which the files are stored
	 */
	public void setTarget(final String target);

	/**
	 * Sets the property in the RDF model which will be used to create the file
	 * names main part. This should be a unique value because if the generated
	 * filename is already existing the file would be overwritten."
	 * 
	 * @param property the property in the RDF model. The object of this property
	 *          will be the main part of the file's name.
	 */
	public void setProperty(final String property);

	/**
	 * Sets the file's suffix.
	 * 
	 * @param fileSuffix the suffix used for the to be generated files
	 */
	public void setFileSuffix(final String fileSuffix);

	/**
	 * Sets the beginning of the index in the filename to extract the name of the
	 * subfolder.
	 * 
	 * @param startIndex This marks the index'beginning.
	 */
	public void setStartIndex(final int startIndex);

	/**
	 * Sets the end of the index in the filename to extract the name of the
	 * subfolder.
	 * 
	 * @param endIndex This marks the index' end.
	 */
	public void setEndIndex(final int endIndex);

	/**
	 * Provides functions and fields which all the {@FilenameExtractor
	 * } interface implementing classes may found useful.
	 * 
	 * @author Pascal Christoph (dr0i)
	 * 
	 */
	public class FilenameUtil {
		String target = "tmp";
		String encoding = "UTF-8";
		String property = null;
		String fileSuffix = null;
		int startIndex = 0;
		int endIndex = 0;
		String filename = "test";

		/**
		 * Ensures that path exists. If not, make it.
		 * 
		 * @param path the path of the to be stored file.
		 */
		public void ensurePathExists(final String path) {
			final File parent = new File(path).getAbsoluteFile().getParentFile();
			parent.mkdirs();
		}
	}
}