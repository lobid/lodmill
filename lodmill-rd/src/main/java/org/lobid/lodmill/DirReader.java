package org.lobid.lodmill;

import java.io.File;

import org.culturegraph.mf.exceptions.MetafactureException;
import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.slf4j.LoggerFactory;

/**
 * Reads a directory and emits all filenames found.
 * 
 * @author Markus Michael Geipel, Pascal Christoph
 */
@In(String.class)
@Out(String.class)
@Description("Reads a directory and emits all filenames found.")
public final class DirReader extends
		DefaultObjectPipe<String, ObjectReceiver<String>> {

	private boolean recursive;

	/**
	 * Set to 'true' if directories should be recursively processed. Default is
	 * 'false' and thus only files residing directly in the directory will be
	 * processed.
	 * 
	 * @param recursive boolean, default is 'false'
	 */
	public void setRecursive(final boolean recursive) {
		this.recursive = recursive;
	}

	@Override
	public void process(final String dir) {
		final File file = new File(dir);
		if (file.isDirectory()) {
			dir(file);
		} else {
			try {
				getReceiver().process(dir);
			} catch (MetafactureException e) {
				LoggerFactory.getLogger(DirReader.class).error(
						"Problems with file '" + file + "'", e);
				getReceiver().resetStream();
			}
		}
	}

	private void dir(final File dir) {
		final File[] files = dir.listFiles();
		for (File file : files) {
			if (file.isDirectory()) {
				if (recursive) {
					dir(file);
				}
			} else {
				process(file.getAbsolutePath());

			}
		}
	}
}