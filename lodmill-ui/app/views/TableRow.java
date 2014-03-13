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
	VALUES, LINK_VALUES, LINKS, IMAGE, LABEL
}