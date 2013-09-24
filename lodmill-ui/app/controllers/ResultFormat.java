/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package controllers;

/**
 * The result format.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public enum ResultFormat {
	/** Use content negotiation to determine the actual result format. */
	NEGOTIATE,
	/** The full JSON representation from the index. */
	FULL,
	/** Short results strings for auto-completion suggestions. */
	SHORT,
	/** JSON maps with 'label' and 'value' fields, where 'value' contains the ID. */
	IDS
}