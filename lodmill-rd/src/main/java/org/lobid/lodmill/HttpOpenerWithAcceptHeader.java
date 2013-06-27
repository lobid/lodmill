package org.lobid.lodmill;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;

import org.culturegraph.mf.exceptions.MetafactureException;
import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.stream.source.Opener;

/**
 * Extension of org.culturegraph.mf.stream.source.HttpOpener. Opens a
 * {@link URLConnection} and passes a reader to the receiver.
 * 
 * @author Christoph Böhme
 * @author Jan Schnasse
 * 
 */
@Description("Opens a http resource and set the accept header. Default accept header is \"accept:*/*\"")
@In(String.class)
@Out(java.io.Reader.class)
public final class HttpOpenerWithAcceptHeader extends
		DefaultObjectPipe<String, ObjectReceiver<Reader>> implements Opener {

	private String encoding = "UTF-8";
	private String accept = "*/*";

	/**
	 * Sets the accept to use when no accept is provided by the server.
	 * 
	 * @param accept The accept header.
	 */
	public void setAccept(final String accept) {
		this.accept = accept;
	}

	/**
	 * Sets the default encoding to use when no encoding is provided by the
	 * server. The default setting is UTF-8.
	 * 
	 * @param encoding new default encoding
	 */
	public void setEncoding(final String encoding) {
		this.encoding = encoding;
	}

	@Override
	public void process(final String urlStr) {
		try {
			final URL url = new URL(urlStr);
			final URLConnection con = url.openConnection();
			con.addRequestProperty("Accept", accept);
			con.addRequestProperty("Accept-Charset", encoding);
			String enc = con.getContentEncoding();
			if (enc == null) {
				enc = encoding;
			}
			getReceiver().process(new InputStreamReader(con.getInputStream(), enc));
		} catch (IOException e) {
			throw new MetafactureException(e);
		}
	}
}
