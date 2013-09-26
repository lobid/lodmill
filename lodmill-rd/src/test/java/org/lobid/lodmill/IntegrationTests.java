/* Copyright 2013 Pascal Christoph, hbz. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Main test suite for all integration tests.
 * 
 * @author Pacsal Christoph (dr0ide)
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ LobidOrganisationsOaiPmhUpdateOnlineTest.class,
		OaipmhZdbOrganisationTest.class })
public final class IntegrationTests {
	/* Suite class, groups tests via annotation above */
}