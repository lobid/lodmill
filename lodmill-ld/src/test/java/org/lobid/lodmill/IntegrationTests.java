/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.lobid.lodmill.hadoop.IntegrationTestCollectSubjects;
import org.lobid.lodmill.hadoop.IntegrationTestIndexFromHdfsInElasticSearch;
import org.lobid.lodmill.hadoop.IntegrationTestLobidNTriplesToJsonLd;

/**
 * Main test suite for all integration tests.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ IntegrationTestIndexFromHdfsInElasticSearch.class,
		IntegrationTestCollectSubjects.class,
		IntegrationTestLobidNTriplesToJsonLd.class })
public final class IntegrationTests {
	/* Suite class, groups tests via annotation above */
}