/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package views;

/**
 * Different ways of serializing a table row (used fo RDFa output)
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
/* no javadoc for elements. */
public enum TableRow {
	SINGLE_VALUE, SINGLE_LINK, MULTI_VALUE, MULTI_LINK, SINGLE_IMAGE
}