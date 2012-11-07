/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.lobid.lodmill.hadoop.JsonLdConverterTests;
import org.lobid.lodmill.hadoop.NTriplesToJsonLdTests;
import org.lobid.lodmill.hadoop.ResolveGndUrisInLobidNTriplesTests;
import org.lobid.lodmill.sparql.GndTests;
import org.lobid.lodmill.sparql.LobidTests;

/**
 * Main test suite for all unit tests.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ NTriplesToJsonLdTests.class,
		ResolveGndUrisInLobidNTriplesTests.class, GndTests.class,
		LobidTests.class, JsonLdConverterTests.class })
public final class UnitTests {
}