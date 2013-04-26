/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Main test suite for all unit tests.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ TransformationZVDDTests.class,
		LobidOrganisationPicaIngestTest.class, ZvddMarcIngestTest.class })
public final class UnitTests {
	/* Suite class, groups tests via annotation above */
}