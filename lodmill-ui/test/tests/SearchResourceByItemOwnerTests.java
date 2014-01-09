/* Copyright 2014 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package tests;

import static org.fest.assertions.Assertions.assertThat;

import java.util.List;

import models.Document;
import models.Index;
import models.Parameter;
import models.Search;

import org.junit.Test;

/**
 * Tests for searching resources by author names, narrowed to item owners.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public class SearchResourceByItemOwnerTests extends SearchTestsHarness {

	private static final String AUTHOR1 = "Hundt, Theo";
	private static final String AUTHOR2 = "Goeters, Johann";

	@Test
	public void searchResByAuthor1_withOwnerId() {
		searchResByAuthorWithOwnerId(AUTHOR1, "DE-Sol1",
				"http://lobid.org/resource/BT000001260");
	}

	@Test
	public void searchResByAuthor1_withOwnerUri() {
		searchResByAuthorWithOwnerId(AUTHOR1,
				"http://lobid.org/organisation/DE-Sol1",
				"http://lobid.org/resource/BT000001260");
	}

	@Test
	public void searchResByAuthor1_withMultipleOwnerIds() {
		searchResByAuthorWithOwnerId(AUTHOR1, "DE-Sol1,DE-Sol2",
				"http://lobid.org/resource/BT000001260");
	}

	@Test
	public void searchResByAuthor1_withMultipleOwnerUris() {
		searchResByAuthorWithOwnerId(
				AUTHOR1,
				"http://lobid.org/organisation/DE-Sol1,http://lobid.org/organisation/DE-Sol2",
				"http://lobid.org/resource/BT000001260");
	}

	@Test
	public void searchResByAuthor2_withOwnerId() {
		searchResByAuthorWithOwnerId(AUTHOR2, "DE-Sol2",
				"http://lobid.org/resource/BT000013654");
	}

	@Test
	public void searchResByAuthor2_withOwnerUri() {
		searchResByAuthorWithOwnerId(AUTHOR2,
				"http://lobid.org/organisation/DE-Sol2",
				"http://lobid.org/resource/BT000013654");
	}

	@Test
	public void searchResByAuthor2_withMultipleOwnerIds() {
		searchResByAuthorWithOwnerId(AUTHOR2, "DE-Sol1,DE-Sol2",
				"http://lobid.org/resource/BT000013654");
	}

	@Test
	public void searchResByAuthor2_withMultipleOwnerUris() {
		searchResByAuthorWithOwnerId(
				AUTHOR2,
				"http://lobid.org/organisation/DE-Sol1,http://lobid.org/organisation/DE-Sol2",
				"http://lobid.org/resource/BT000013654");
	}

	private static void searchResByAuthorWithOwnerId(String author,
			String holder, String resultId) {
		final List<Document> docs =
				new Search(author, Index.LOBID_RESOURCES, Parameter.AUTHOR).owner(
						holder).documents();
		assertThat(docs.size()).isEqualTo(1);
		assertThat(docs.get(0).getId()).isEqualTo(resultId);
	}
}
