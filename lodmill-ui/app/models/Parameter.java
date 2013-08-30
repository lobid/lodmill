/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models;

/**
 * Parameters for API requests.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public enum Parameter {
	ID, NAME, AUTHOR, SUBJECT, SET;
	/**
	 * @return The parameter id (the string passed to the API)
	 */
	public String id() { // NOPMD
		return name().toLowerCase();
	}
}
