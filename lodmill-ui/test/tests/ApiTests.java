/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package tests;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.test.Helpers.running;
import static play.test.Helpers.testServer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
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
				{ "resource?author=Melville",/* -> */"a peep at Polynesian life" },
				{ "resource?author=Melville&format=short",/* -> */"Melville, Herman" },
				{ "resource?author=118580604",/* -> */"a peep at Polynesian life" },
				{ "resource?subject=4414195-6",/* -> */"aus dem Kreis Olpe" },
				/* search by dewey broken, see #119 */
				// { "resource?subject=Chemistry",/* -> */"Synthese, Eigenschaften" },
				// { "resource?subject=Chemistry&format=short",/* -> */
				// "Chemistry & allied sciences" },
				{ "resource?name=Typee&format=ids",/* -> */
				"http://lobid.org/resource/HT002189125" },
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
				{ "organisation?name=hbz&format=ids",/* -> */
				"http://lobid.org/organisation/DE-605" },
				/*-------------*/
				/* GET /person */
				/*-------------*/
				{ "person/136963781",/* -> */"Bach, Johann Sebastian" }, // NOPMD
				{ "person?id=136963781",/* -> */"Bach, Johann Sebastian" },
				{ "person?name=Bach",/* -> */"Bach, Johann Sebastian" },
				{ "person?name=Bach&format=short",/* -> */
				"Bach, Johann Sebastian (1685-1750)" },
				{ "person?name=Bach&format=ids", /* -> */
				"http://d-nb.info/gnd/11850553X" },
				/*-------------*/
				/* GET /search */
				/*-------------*/
				{ "search?name=Bas",/* -> */"Bach, Johann Sebastian" },
				{ "search?name=Bas",/* -> */"Universität Basel" },
				{ "search?name=Bas&format=ids",/* -> */
				"http://lobid.org/organisation/SzBaU" }
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

	@BeforeClass
	public static void setup() throws IOException, InterruptedException {// NOPMD
		SearchTests.setup();
	}

	@AfterClass
	public static void down() {
		SearchTests.down();
	}

	@Test
	public void callEndpointAndCheckResponse() {
		running(testServer(5000), new Runnable() {
			@Override
			public void run() {
				final String response = SearchTests.call(endpoint);
				assertNotNull("Expecting non-null when calling: " + endpoint, response);
				assertTrue(String.format(
						"Response to calling endpoint '%s' should contain content '%s'",
						endpoint, content), response.contains(content));
			}
		});
	}

}
