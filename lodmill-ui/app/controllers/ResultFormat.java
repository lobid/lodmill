/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package controllers;

/**
 * The result format.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public enum ResultFormat {
	/** Complete HTML page with search form on top, results at bottom. */
	PAGE,
	/** The fulle JSON representation from the index. */
	FULL,
	/** Short results strings for auto-completion suggestions. */
	SHORT
}