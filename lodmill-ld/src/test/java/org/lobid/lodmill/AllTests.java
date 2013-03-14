/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Main test suite for all tests.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ UnitTests.class, IntegrationTests.class })
public final class AllTests {
	/* Suite class, groups tests via annotation above */
}