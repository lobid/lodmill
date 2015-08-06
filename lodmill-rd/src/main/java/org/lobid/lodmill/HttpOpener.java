/*
 *  Copyright 2013 Deutsche Nationalbibliothek
 *
 *  Licensed under the Apache License, Version 2.0 the "License";
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.lobid.lodmill;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;

import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.slf4j.LoggerFactory;

/**
 * Opens a {@link URLConnection} and passes a reader to the receiver. If the URL
 * connection eturns a HTTP status code of 420 the programm waits for some
 * seconds before going on.
 * 
 * @author Christoph Böhme
 * @author Jan Schnasse
 * @author Pascal Christoph (dr0i)
 * 
 */
@Description("Opens a http resource. Supports the setting of Accept and Accept-Charset as http header fields.")
@In(String.class)
@Out(java.io.Reader.class)
public final class HttpOpener
		extends DefaultObjectPipe<String, ObjectReceiver<Reader>>
		implements org.culturegraph.mf.stream.source.Opener {
	private String encoding = "UTF-8";
	private String accept = "*/*";
	private final long MILLISECONDS_TO_WAIT = 5000L;

	/**
	 * @param accept The accept header in the form type/subtype, e.g. text/plain.
	 */
	public void setAccept(final String accept) {
		this.accept = accept;
	}

	/**
	 * @param encoding The encoding is used to encode the output and is passed as
	 *          Accept-Charset to the http connection.
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
			LoggerFactory.getLogger(HttpOpener.class)
					.error("Problems with URL '" + urlStr + "'", e.getLocalizedMessage());
			if (e.getLocalizedMessage().contains("420")) {
				try {
					LoggerFactory.getLogger(HttpOpener.class)
							.info("Wait for " + MILLISECONDS_TO_WAIT / 1000 + " sec.");
					Thread.sleep(MILLISECONDS_TO_WAIT);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
	}
}
