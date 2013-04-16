/* Copyright 2013 hbz, Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import org.culturegraph.mf.stream.reader.XmlReaderBase;

/**
 * @author Pascal Christoph
 */

public class PicaXmlReader extends XmlReaderBase {
	/**
	 * Create a reader for pica XML.
	 */
	public PicaXmlReader() {
		super(new PicaXmlHandler());
	}
}