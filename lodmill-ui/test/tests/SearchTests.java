/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package tests;

import static org.fest.assertions.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import models.Document;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;

import play.api.mvc.SimpleResult;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Http.Status;
import play.mvc.Result;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

import controllers.Application;

/**
 * Tests for the search functionality.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class SearchTests {

	static final String TERM = "ray";

	@Test
	public void searchViaModel() {
		final List<Document> docs = Document.search(TERM, "gnd-index");
		assertThat(docs.size()).isPositive();
		for (Document document : docs) {
			assertThat(document.matchedField.toLowerCase()).contains(TERM);
		}
	}

	@Test
	public void searchViaModelBirth() {
		assertThat(
				Document.search("Schmidt, Karla (1974-)", "gnd-index").size())
				.isEqualTo(1);
	}

	@Test
	public void searchViaModelBirthDeath() {
		assertThat(
				Document.search("Schmidt, Karl (1902-1945)", "gnd-index")
						.size()).isEqualTo(1);
	}

	@Test
	public void searchViaController() {
		final Map<String, String> data = Collections.emptyMap();
		Http.Context.current.set(new Http.Context(null, data, data));
		final Result result = Application.autocomplete(TERM);
		System.out.println(result.getWrappedResult().getClass());
		assertThat(
				((SimpleResult<?>) result.getWrappedResult()).header().status())
				.isEqualTo(Status.OK);
	}

	@Test
	public void searchViaApiPageEmpty() throws IOException {
		assertThat(call("")).contains("<html>");

	}

	@Test
	public void searchViaApiPage() throws IOException {
		assertThat(call("?index=lobid-index&query=ferdi&format=page"))
				.contains("<html>");

	}

	@Test
	public void searchViaApiFull() throws IOException {
		final JsonNode jsonObject =
				Json.parse(call("?index=lobid-index&query=ferdi&format=full"));
		assertThat(jsonObject.isArray()).isTrue();
		assertThat(jsonObject.size()).isGreaterThan(10);
		assertThat(jsonObject.getElements().next().isContainerNode()).isTrue();
	}

	@Test
	public void searchViaApiShort() throws IOException {
		final JsonNode jsonObject =
				Json.parse(call("?index=lobid-index&query=ferdi&format=short"));
		assertThat(jsonObject.isArray()).isTrue();
		assertThat(jsonObject.size()).isGreaterThan(10);
		assertThat(jsonObject.getElements().next().isContainerNode()).isFalse();
	}

	private String call(final String request) throws IOException,
			MalformedURLException {
		return CharStreams.toString(new InputStreamReader(new URL(
				"http://localhost:7000/search" + request).openStream(),
				Charsets.UTF_8));
	}
}
