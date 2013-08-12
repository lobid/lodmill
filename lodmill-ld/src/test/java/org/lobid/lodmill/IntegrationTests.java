/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.lobid.lodmill.hadoop.IndexFromHdfsInElasticSearchTests;
import org.lobid.lodmill.hadoop.ResolveBlankNodesOnMiniClusterTests;

/**
 * Main test suite for all integration tests.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ IndexFromHdfsInElasticSearchTests.class,
		ResolveBlankNodesOnMiniClusterTests.class })
public final class IntegrationTests {
	/* Suite class, groups tests via annotation above */
}