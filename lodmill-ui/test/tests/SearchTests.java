/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package tests;

import static org.fest.assertions.Assertions.assertThat;

import java.util.List;

import models.Document;

import org.junit.Test;

import play.api.mvc.SimpleResult;
import play.mvc.Http.Status;
import play.mvc.Result;
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
		final List<Document> docs = Document.search(TERM, Document.esIndex);
		assertThat(docs.size()).isPositive();
		for (Document document : docs) {
			assertThat(document.author.toLowerCase()).contains(TERM);
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
		final Result result = Application.autocompleteSearch(TERM);
		System.out.println(result.getWrappedResult().getClass());
		assertThat(
				((SimpleResult<?>) result.getWrappedResult()).header().status())
				.isEqualTo(Status.OK);
	}
}
