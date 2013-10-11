/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * Parameters for API requests.
 * 
 * @author Fabian Steeg (fsteeg)
 */
@SuppressWarnings("javadoc")
public enum Parameter {
	ID, NAME, AUTHOR, SUBJECT, SET, Q;
	/**
	 * @return The parameter id (the string passed to the API)
	 */
	public String id() { // NOPMD
		return name().toLowerCase();
	}

	public static Map.Entry<Parameter, String> select(
			ImmutableMap<Parameter, String> params) {
		for (Map.Entry<Parameter, String> p : params.entrySet())
			if (isDefined(p.getValue()))
				return p;
		return null;
	}

	private static boolean isDefined(final String param) {
		return !param.isEmpty();
	}
}
