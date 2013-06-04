/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import org.culturegraph.mf.test.TestSuite;
import org.culturegraph.mf.test.TestSuite.TestDefinitions;
import org.junit.runner.RunWith;

/**
 * @author Fabian Steeg
 */
@RunWith(TestSuite.class)
@TestDefinitions({ "TransformationZvdd-title-print.xml",
		"TransformationZvdd-title-digital.xml", "TransformationZvdd-collection.xml" })
public final class TransformationZVDDTests {
	/* Suite class, groups tests via annotation above */
}
