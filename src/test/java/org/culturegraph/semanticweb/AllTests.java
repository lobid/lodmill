/* Copyright 2012 Fabian Steeg. Licensed under the Apache License Version 2.0 */

package org.culturegraph.semanticweb;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Main test suite for all tests.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ GutenbergTests.class, GndTests.class, LobidTests.class })
public final class AllTests {
}