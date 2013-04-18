/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package tests;

import static org.fest.assertions.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

import models.Document;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;

import play.libs.Json;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

/**
 * Tests for the search functionality.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public class SearchTests {

	static final String TERM = "abr";

	@Test
	public void searchViaModel() {
		final List<Document> docs = Document.search(TERM, "lobid-index", "author");
		assertThat(docs.size()).isPositive();
		for (Document document : docs) {
			assertThat(document.matchedField.toLowerCase()).contains(TERM);
		}
	}

	@Test
	public void searchViaModelBirth() {
		assertThat(
				Document.search("Abrahamson, Mark (1939-)", "lobid-index", "author")
						.size()).isEqualTo(1);
	}

	@Test
	public void searchViaModelBirthDeath() {
		assertThat(
				Document.search("Abrahams, Israel (1858-1925)", "gnd-index", "author")
						.size()).isEqualTo(1);
	}

	@Test
	public void searchViaApiPageEmpty() throws IOException {
		assertThat(call("")).contains("<html>");
	}

	@Test
	public void searchViaApiPage() throws IOException {
		assertThat(
				call("search?index=lobid-index&query=abraham&format=page&category=author"))
				.contains("<html>");

	}

	@Test
	public void searchViaApiFull() throws IOException {
		final JsonNode jsonObject =
				Json.parse(call("search?index=lobid-index&query=abraham&format=full&category=author"));
		assertThat(jsonObject.isArray()).isTrue();
		assertThat(jsonObject.size()).isGreaterThan(10);
		assertThat(jsonObject.getElements().next().isContainerNode()).isTrue();
	}

	@Test
	public void searchViaApiShort() throws IOException {
		final JsonNode jsonObject =
				Json.parse(call("search?index=lobid-index&query=abraham&format=short&category=author"));
		assertThat(jsonObject.isArray()).isTrue();
		assertThat(jsonObject.size()).isGreaterThan(10);
		assertThat(jsonObject.getElements().next().isContainerNode()).isFalse();
	}

	@Test
	public void searchViaApiWithContentNegotiation() throws IOException {
		final String nTriples = call("author/abraham", "text/plain");
		final String turtle = call("author/abraham", "text/turtle");
		final String n3 = call("author/abraham", "text/n3"); // NOPMD
		assertThat(nTriples).isNotEmpty();
		assertThat(turtle).isNotEmpty();
		assertThat(n3).isNotEmpty();
		assertThat(nTriples).isNotEqualTo(turtle);
		assertThat(turtle).isEqualTo(n3); /* turtle is a subset of n3 for RDF */
	}

	private static String call(final String request) throws IOException,
			MalformedURLException {
		return call(request, "application/json");
	}

	private static String call(final String request, final String contentType)
			throws IOException, MalformedURLException {
		final URLConnection url =
				new URL("http://localhost:7000/" + request).openConnection();
		url.setRequestProperty("Accept", contentType);
		return CharStreams.toString(new InputStreamReader(url.getInputStream(),
				Charsets.UTF_8));
	}
}
