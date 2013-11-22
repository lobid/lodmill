/* Copyright 2013 hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ReaderInputStream;
import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

/**
 * Opens (aka 'untar') a tar archive and passes every entry .
 * 
 * @author Pascal Christoph (dr0i)
 */
@Description("Opens a tar archive and passes every entry.")
@In(Reader.class)
@Out(Reader.class)
public class TarReader extends
		DefaultObjectPipe<Reader, ObjectReceiver<Reader>> {
	@Override
	public void process(final Reader reader) {
		TarArchiveInputStream tarInputStream = null;
		try {
			tarInputStream = new TarArchiveInputStream(new ReaderInputStream(reader));
			TarArchiveEntry entry = null;
			while ((entry = (TarArchiveEntry) tarInputStream.getNextEntry()) != null) {
				if (!entry.isDirectory()) {
					byte[] buffer = new byte[(int) entry.getSize()];
					while ((tarInputStream.read(buffer)) > 0) {
						getReceiver().process(new StringReader(new String(buffer)));
					}
				}
			}
			tarInputStream.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeQuietly(tarInputStream);
		}
	}
}
