package org.lobid.lodmill;

/**
 * This interface declares methods for setting variables used by all RDF sinks.
 * 
 * @author dr0i
 * 
 */
public interface RDFSink {

	/**
	 * 
	 * @param serialization Sets the serialization format.
	 */
	public void setSerialization(final String serialization);
}
