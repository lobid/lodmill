package controllers;

/** Different ways of serializing a table row (used fo RDFa output) */
@SuppressWarnings("javadoc")
/* no javadoc for elements */
public enum TableRow {
	SINGLE_VALUE, SINGLE_LINK, MULTI_VALUE, MULTI_LINK, SINGLE_IMAGE
}