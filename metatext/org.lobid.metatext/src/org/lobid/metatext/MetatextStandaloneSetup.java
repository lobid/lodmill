
package org.lobid.metatext;

/**
 * Initialization support for running Xtext languages 
 * without equinox extension registry
 */
public class MetatextStandaloneSetup extends MetatextStandaloneSetupGenerated{

	public static void doSetup() {
		new MetatextStandaloneSetup().createInjectorAndDoEMFRegistration();
	}
}

