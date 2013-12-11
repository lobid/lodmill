/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.lobid.lodmill.hadoop.UnitTestCollectSubjects;
import org.lobid.lodmill.hadoop.UnitTestGndNTriplesToJsonLd;
import org.lobid.lodmill.hadoop.UnitTestItemNTriplesToJsonLd;
import org.lobid.lodmill.hadoop.UnitTestJsonLdConverter;
import org.lobid.lodmill.hadoop.UnitTestJsonLdConverterWithBlankNodes;
import org.lobid.lodmill.hadoop.UnitTestLobidNTriplesToJsonLd;

/**
 * Main test suite for all unit tests.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ UnitTestLobidNTriplesToJsonLd.class,
		UnitTestGndNTriplesToJsonLd.class, UnitTestCollectSubjects.class,
		UnitTestJsonLdConverter.class, UnitTestJsonLdConverterWithBlankNodes.class,
		UnitTestItemNTriplesToJsonLd.class })
public final class UnitTests {
	/* Suite class, groups tests via annotation above */
}