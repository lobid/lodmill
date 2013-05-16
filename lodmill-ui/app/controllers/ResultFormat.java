package controllers;

/**
 * The result format.
 */
public enum ResultFormat {
	/** Complete HTML page with search form on top, results at bottom. */
	PAGE,
	/** The fulle JSON representation from the index. */
	FULL,
	/** Short results strings for auto-completion suggestions. */
	SHORT
}