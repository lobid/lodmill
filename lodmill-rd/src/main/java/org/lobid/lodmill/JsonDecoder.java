/* Copyright 2014 hbz, Pascal Christoph.
 * Licensed under the Eclipse Public License 1.0 */
package org.lobid.lodmill;

import java.io.IOException;
import java.io.Reader;

import org.culturegraph.mf.framework.DefaultObjectPipe;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.io.CharStreams;

/**
 * Decodes JSON and JSONP. The JSON (or JSONP) record may consist of one or n
 * records. However, if there are objects in arrays, these will be handled as
 * new objects and not as part of the root object.
 * 
 * @author Pascal Christoph (dr0i)
 * 
 */
@Description("Decodes a json(p) record as literals (as key-value pairs).")
@In(Reader.class)
@Out(StreamReceiver.class)
public final class JsonDecoder extends
		DefaultObjectPipe<Reader, StreamReceiver> {
	private static final String JSON_START_CHAR = "{";
	private static final String JSON_CALLBACK = "json_callback";
	private JsonParser jsonParser;
	private static final Logger LOG = LoggerFactory.getLogger(JsonDecoder.class);
	private boolean STARTED;
	private boolean JSONP;

	private void handleValue(final JsonToken currentToken, final String key)
			throws IOException, JsonParseException {
		{
			if (JsonToken.VALUE_STRING == currentToken
					|| JsonToken.VALUE_NUMBER_INT == currentToken
					|| JsonToken.VALUE_NUMBER_FLOAT == currentToken) {
				final String value = this.jsonParser.getText();
				LOG.debug("key=" + key + " value=" + value);
				getReceiver().literal(key, value);
			}
		}
	}

	@Override
	public void process(final Reader reader) {
		STARTED = false;
		LOG.debug("############################ New");
		try {
			JsonToken currentToken = parseJson(reader);
			if (currentToken == null)
				return;
			processTokens(currentToken);
		} catch (final IOException e) {
			try {
				LOG.warn(e.getLocalizedMessage() + "while computing "
						+ this.jsonParser.getText());
			} catch (JsonParseException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}

	private JsonToken parseJson(final Reader reader) throws IOException,
			JsonParseException {
		String text = CharStreams.toString(reader);
		this.jsonParser = new JsonFactory().createParser(text);
		JsonToken currentToken = null;
		try {
			currentToken = this.jsonParser.nextToken();
		} catch (final JsonParseException e) {
			// is it JSONP ?
			if (text.indexOf(JsonDecoder.JSON_START_CHAR) == -1) {
				LOG.info("No JSON(P) - ignoring");
				return null;
			}
			currentToken = handleJsonp(text);
		}
		while (JsonToken.START_OBJECT != currentToken) {
			this.jsonParser.nextToken();
		}
		return currentToken;
	}

	private void processTokens(JsonToken token) throws IOException,
			JsonParseException {
		JsonToken currentToken = token;
		while (currentToken != null) {
			if (JsonToken.START_OBJECT == currentToken) {
				if (!STARTED) {
					getReceiver().startRecord("");
					STARTED = true;
				}
				currentToken = processRecordContent(this.jsonParser.nextToken());
			}
			LOG.debug("############################ End");
			if (STARTED) {
				getReceiver().endRecord();
				STARTED = false;
			}
		}
	}

	private JsonToken processRecordContent(JsonToken token) throws IOException,
			JsonParseException {
		JsonToken currentToken = token;
		String key = null;
		while (currentToken != null) {
			if (JsonToken.FIELD_NAME == currentToken)
				key = this.jsonParser.getCurrentName();
			if (JsonToken.START_ARRAY == currentToken) {
				currentToken = this.jsonParser.nextToken();
				if (this.JSONP)
					currentToken = this.jsonParser.nextToken();
				else {
					// break to treat objects in arrays as new objects
					if (JsonToken.START_OBJECT == currentToken)
						break;
					currentToken = handleValuesOfArrays(currentToken, key);
				}
			}
			if (JsonToken.START_OBJECT == currentToken) {
				if (this.jsonParser.getCurrentName() == null)
					break;
			} else
				handleValue(currentToken, key);
			try {
				currentToken = this.jsonParser.nextToken();
			} catch (JsonParseException e) {
				LOG.debug("Exception at the end of non JSON object, might be JSONP", e);
				currentToken = null;
				break;
			}
		}
		return currentToken;
	}

	private JsonToken handleValuesOfArrays(final JsonToken currentToken,
			final String key) throws JsonParseException, IOException {
		int i = 0;
		JsonToken jtoken = currentToken;
		while (JsonToken.END_ARRAY != currentToken) {
			final String value = this.jsonParser.getText();
			LOG.debug("key=" + key + i + " valueArray=" + value);
			getReceiver().literal(key + i, value);
			jtoken = this.jsonParser.nextToken();
			i++;
		}
		return jtoken;
	}

	private JsonToken handleJsonp(final String jsonp) throws IOException,
			JsonParseException {
		JsonToken currentToken;
		final String callbackString =
				jsonp.substring(0, jsonp.indexOf(JsonDecoder.JSON_START_CHAR) - 1);
		final String json =
				jsonp.substring(jsonp.indexOf(JsonDecoder.JSON_START_CHAR),
						jsonp.length() - 1);
		this.jsonParser = new JsonFactory().createParser(json);
		LOG.debug("key=" + JsonDecoder.JSON_CALLBACK + " value=" + callbackString);
		getReceiver().startRecord("");
		STARTED = true;
		JSONP = true;
		getReceiver().literal(JsonDecoder.JSON_CALLBACK, callbackString);
		LOG.debug("Json=" + json);
		currentToken = this.jsonParser.nextToken();
		return currentToken;
	}
}
