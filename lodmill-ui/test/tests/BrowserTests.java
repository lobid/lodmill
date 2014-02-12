/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package tests; // NOPMD

import static org.fest.assertions.Assertions.assertThat;
import static org.fluentlenium.core.filter.FilterConstructor.withId;
import static org.fluentlenium.core.filter.FilterConstructor.withText;
import static play.test.Helpers.HTMLUNIT;
import static play.test.Helpers.running;
import static tests.SearchTestsHarness.call;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import play.libs.F.Callback;
import play.test.TestBrowser;
import play.test.TestServer;

/**
 * Browser-based tests using Selenium WebDriver and FluentLenium.
 * <p/>
 * Run with `play test` (currently doesn't work in Eclipse).
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public class BrowserTests {

	private static final String API_PAGE = "http://localhost:"
			+ SearchTestsHarness.TEST_SERVER_PORT + "/api";
	private static final TestServer TEST_SERVER = SearchTestsHarness.TEST_SERVER;

	@BeforeClass
	public static void setup() throws IOException { // NOPMD
		SearchTestsHarness.setup();
	}

	@AfterClass
	public static void down() {
		SearchTestsHarness.down();
	}

	@Test
	public void accessAssets() { /* works with play test, but not in eclipse */
		running(TEST_SERVER, new Runnable() {
			@Override
			public void run() {
				assertThat(call("assets/javascripts/bootstrap.min.js")).isNotNull();
			}
		});
	}

	@Test
	public void queryForm() {
		running(TEST_SERVER, HTMLUNIT, new Callback<TestBrowser>() {
			@Override
			public void invoke(final TestBrowser browser) throws InterruptedException {
				browser.goTo(API_PAGE);
				assertThat(browser.title()).isEqualTo("lobid - api");
				browser.find("input", withId("id")).text(
						"http://d-nb.info/gnd/118836617");
				browser.find("button", withText("Search")).first().click();
				assertThat(browser.url()).isNotEqualTo(API_PAGE);
				assertThat(browser.pageSource()).contains("118836617")
						.contains("Schmidt, Hannelore").contains("Glaser, Hannelore")
						.contains("Schmidt, Loki");
			}
		});
	}

	@Test
	public void sampleRequestResourceById() {
		running(TEST_SERVER, HTMLUNIT, new Callback<TestBrowser>() {
			@Override
			public void invoke(final TestBrowser browser) {
				browser.goTo(API_PAGE);
				browser.find("a", withText("hbz ID")).first().click();
				assertTypee(browser);
			}
		});
	}

	private static void assertTypee(final TestBrowser browser) {
		assertThat(browser.pageSource()).contains("Typee")
				.contains("a peep at Polynesian life")
				.contains("http://gutenberg.org/ebooks/9269")
				.contains("http://gutenberg.org/ebooks/23969")
				.contains("http://openlibrary.org/works/OL14953734W")
				.contains("http://dbpedia.org/resource/Typee")
				.contains("http://lobid.org/item/HT002189125%3AU+MEL-11")
				.contains("exemplar");
	}

	@Test
	public void sampleRequestResourceByAuthor() {
		running(TEST_SERVER, HTMLUNIT, new Callback<TestBrowser>() {
			@Override
			public void invoke(final TestBrowser browser) {
				browser.goTo(API_PAGE);
				browser.click("a", withText("/resource?author=Abramson"));
				assertThat(browser.pageSource()).contains("Abramson")
						.contains("Error-correcting codes from linear sequential circuits")
						.contains("The ethnic factor in American catholicism")
						.contains("an analysis of interethnic marriage")
						.contains("Abramson, Harold J.").contains("Abramson, N. M.");
			}
		});
	}

	@Test
	public void sampleRequestResourceBySubjectId() {
		running(TEST_SERVER, HTMLUNIT, new Callback<TestBrowser>() {
			@Override
			public void invoke(final TestBrowser browser) {
				browser.goTo(API_PAGE);
				browser.click("a", withText("/resource?subject=4414195-6"));
				assertThat(browser.pageSource())
						.contains("Heimatstimmen aus dem Kreis Olpe")
						.contains(
								"Ein Papierkrieg um die Instandsetzung des Wittgensteiner "
										+ "Kohlenwegs in der Gemarkung Heinsberg 1837 bis 1839 : die "
										+ "Heinsberger machten Schwierigkeiten")
						.contains("Hundt, Theo");
			}
		});
	}

	@Test
	public void sampleRequestResourceBySubjectLabel() {
		running(TEST_SERVER, HTMLUNIT, new Callback<TestBrowser>() {
			@Override
			public void invoke(final TestBrowser browser) {
				browser.goTo(API_PAGE);
				browser.click("a", withText("/resource?subject=Chemistry"));
				assertThat(browser.pageSource()).contains("Gerlach, Rainer").contains(
						"Synthese, Eigenschaften und Reaktivität von Mangan- und "
								+ "Chrom-Komplexen mit Stickstoff-Liganden");
			}
		});
	}

	@Test
	public void sampleRequestOrganisationByTitle() {
		running(TEST_SERVER, HTMLUNIT, new Callback<TestBrowser>() {
			@Override
			public void invoke(final TestBrowser browser) {
				browser.goTo(API_PAGE);
				browser.click("a", withText("/organisation?name=Universität"));
				assertThat(browser.pageSource())
						.contains("Universität")
						.contains("Universität Basel")
						.contains("Ruhr-Universität Bochum, Universitätsbibliothek")
						.contains("Universitätsstr. 150")
						.contains("http://www.ub.ruhr-uni-bochum.de")
						.contains("mailto:benutzung.ub@ruhr-uni-bochum.de")
						.contains(
								"http://de.wikipedia.org/wiki/Universitätsbibliothek_Bochum");
			}
		});
	}

	@Test
	public void sampleRequestGndByAuthor() {
		running(TEST_SERVER, HTMLUNIT, new Callback<TestBrowser>() {
			@Override
			public void invoke(final TestBrowser browser) {
				browser.goTo(API_PAGE);
				final String link = "/person?name=Johann+Sebastian+Bach";
				browser.click("a", withText(link));
				assertThat(browser.pageSource()).contains("Bach, Johann Sebastian")
						.contains("Lithograph, tätig in Leipzig um 1835-1837")
						.contains("Bruder von Marie Salome Bach, spätere Wiegand")
						.contains("Bach, Johann Samuel").contains("Bach, Jean-Sébastien")
						.contains("Bach, Joh. Sebst.").contains("Dt. Maler");
			}
		});
	}
}
