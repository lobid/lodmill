/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package tests;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.test.Helpers.running;
import static play.test.Helpers.testServer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests for API endpoints.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
@RunWith(value = Parameterized.class)
public class ApiTests {

	private final String endpoint;
	private final String content;

	/**
	 * @return The data to use for this parameterized test (test is executed once
	 *         for every element, which is passed to the constructor of this test)
	 */
	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
				/*---------------*/
				/* GET /resource */
				/*---------------*/
				{ "resource/HT002189125",/* -> */"a peep at Polynesian life" }, // NOPMD
				{ "resource?id=HT002189125",/* -> */"a peep at Polynesian life" },
				{ "resource?name=Typee",/* -> */"a peep at Polynesian life" },
				{ "resource?name=Typee&t=Book", "a peep at Polynesian life" },
				{ "resource?author=Melville",/* -> */"a peep at Polynesian life" },
				{ "resource?author=Melville&format=short",/* -> */
				"Melville, Herman" },
				{ "resource?author=118580604",/* -> */"a peep at Polynesian life" },
				{ "resource?subject=4414195-6",/* -> */"aus dem Kreis Olpe" },
				{ "resource?subject=4414195-6&set=NWBib",/* -> */
				"http://lobid.org/resource/NWBib" },
				{ "resource?subject=Chemistry",/* -> */"Synthese, Eigenschaften" },
				{ "resource?subject=Chemistry&format=short",/* -> */
				"Chemistry & allied sciences" },
				{ "resource?name=Typee&format=ids",/* -> */
				"http://lobid.org/resource/HT002189125" },
				{ "resource?q=Typee&format=ids",/* -> */"Typee" },
				{ "resource?set=NWBib",/* -> */
				"Wann en Kölle de Chress-Stäne blöhe" },
				{ "resource?id=TT002234003",/*->*/
				"Aufgabenbereich und Aufbau der Verwaltung" },
				{ "resource?id=TT002234003&format=ids&callback=response",/*->*/
				"response(" },
				{ "resource?id=TT002234003&format=short&callback=response",/*->*/
				"response(" },
				{ "resource?id=TT002234003&format=full&callback=response",/*->*/
				"response(" },
				{ "resource?author=Hu&owner=DE-Sol1",/*->*/
				"Heimatstimmen aus dem Kreis Olpe" },
				{ "resource?author=Hu&owner=http://lobid.org/organisation/DE-Sol1",/*->*/
				"Heimatstimmen aus dem Kreis Olpe" },
				{ "resource?id=BT000001260&owner=DE-Sol1",/*->*/
				"Heimatstimmen aus dem Kreis Olpe" },
				{ "resource?set=NWBib&owner=DE-Sol1",/*->*/
				"Heimatstimmen aus dem Kreis Olpe" },
				{ "resource?author=Hu&owner=DE-Sol1,DE-Sol2",/*->*/
				"Heimatstimmen aus dem Kreis Olpe" },
				{ "resource?author=Go&owner=DE-Sol1,DE-Sol2",/*->*/
				"Kirchengeschichte des Rheinlandes" },
				/*-------------------*/
				/* GET /organisation */
				/*-------------------*/
				{ "organisation/US-IdBoTIMB",/* -> */"Timberline High School" },
				{ "organisation/SzBaU",/* -> */"Universität Basel" }, // NOPMD
				{ "organisation?id=SzBaU",/* -> */"Universität Basel" },
				{ "organisation?name=Basel",/* -> */"Universität Basel" },
				{ "organisation?name=Basel&format=short",/* -> */
				"Universität Basel" },
				{ "organisation?name=Basel&format=ids",/* -> */
				"http://lobid.org/organisation/SzBaU" },
				{ "organisation?name=hbz",/* -> */"Hochschulbibliothekszentrum" },
				{ "organisation?name=hbz&format=short",/* -> */
				"Hochschulbibliothekszentrum" },
				{ "organisation?name=Hochschulbibliotheksz",/* -> */
				"Hochschulbibliothekszentrum" },
				{ "organisation?id=DE-605",/* -> */"Hochschulbibliothekszentrum" },
				{ "organisation?id=DE-605",/* -> */"Einrichtung ohne Bestand" },
				{ "organisation?id=DE-605",/* -> */"Land" },
				{ "organisation?name=hbz&format=ids",/* -> */
				"http://lobid.org/organisation/DE-605" },
				{ "organisation?q=Einrichtung+ohne+Bestand&format=full",/* -> */
				"Hochschulbibliothekszentrum" },
				{ "organisation?q=Einrichtung+ohne+Bestand&format=ids",/* -> */
				"Hochschulbibliothekszentrum" },
				{ "organisation?q=Einrichtung+ohne+Bestand&format=short",/* -> */
				"Hochschulbibliothekszentrum" },
				{ "organisation?q=Einrichtung+ohne+Bestand&format=negotiate",/* -> */
				"Hochschulbibliothekszentrum" },
				/*-------------*/
				/* GET /person */
				/*-------------*/
				{ "person/136963781",/* -> */"Bach, Johann Sebastian" }, // NOPMD
				{ "person?id=136963781",/* -> */"Bach, Johann Sebastian" },
				{ "person?name=Bach",/* -> */"Bach, Johann Sebastian" },
				{ "person?name=Bach&format=short",/* -> */
				"Bach, Johann Sebastian (1685-1750)" },
				{ "person?q=Bach&format=short&t=DifferentiatedPerson",
						"Bach, Johann Sebastian (1685-1750)" },
				{ "person?q=Bruder+von&format=full",
						"Marie Salome Bach, spätere Wiegand" },
				{ "person?name=Bach&format=ids", /* -> */
				"http://d-nb.info/gnd/11850553X" },
				/*-------------*/
				/* GET /item */
				/*-------------*/
				{ "item?id=BT000000079%3AGA+644",/* -> */"GA 644" },
				{ "item/BT000000079%3AGA+644",/* -> */"GA 644" },
				{ "item?name=GA+644&format=ids",/* -> */
				"http://lobid.org/item/BT000000079%3AGA+644" },
				{ "item?q=GA+644&format=ids",/* -> */
				"http://lobid.org/item/BT000000079%3AGA+644" },
				/*-------------*/
				/* GET /search */
				/*-------------*/
				{ "search?name=Ba",/* -> */"Bach, Johann Sebastian" },
				{ "search?name=Ba",/* -> */"Universität Basel" },
				{ "search?name=Ba&format=ids",/* -> */
				"http://lobid.org/organisation/SzBaU" },
				/*-------------*/
				/* GET /subject */
				/*-------------*/
				{ "subject?id=http://d-nb.info/gnd/7765116-9", "Kirchhundem" },
				{ "subject?id=7765116-9", "Kirchhundem" },
				{ "subject?id=http://d-nb.info/gnd/1706733-9",
						"Herbstadt-Ottelmannshausen" },
				{ "subject?id=1706733-9", "Herbstadt-Ottelmannshausen" },
				{ "subject?name=Ottelmannshausen", "Herbstadt-Ottelmannshausen" },
				{ "subject?name=Kirchhundem", "Kirchhundem-Heinsberg" },
				{ "search?name=Ottelmannshausen", "Herbstadt-Ottelmannshausen" }
		/**/
		});
	}

	/**
	 * @param endpoint The endpoin to call
	 * @param content The content that the response should contain
	 */
	public ApiTests(final String endpoint, final String content) {
		this.endpoint = endpoint;
		this.content = content;
		System.out.println(String.format(
				"Testing if calling endpoint '%s' returns something with content '%s'",
				endpoint, content));
	}

	@Before
	public void setup() throws IOException {// NOPMD
		SearchTestsHarness.setup();
	}

	@After
	public void down() {
		SearchTestsHarness.down();
	}

	@Test
	public void callEndpointAndCheckResponse() {
		running(testServer(5000), new Runnable() {
			@Override
			public void run() {
				final String response = SearchTestsHarness.call(endpoint);
				assertNotNull("Expecting non-null when calling: " + endpoint, response);
				assertTrue(
						String
								.format(
										"Response to calling endpoint '%s' should contain content '%s', but was '%s'",
										endpoint, content, response), response.contains(content));
			}
		});
	}

}
