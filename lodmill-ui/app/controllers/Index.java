package controllers;

/**
 * The different indices to use.
 */
public enum Index {
	/***/
	LOBID_RESOURCES("lobid-index"), /***/
	LOBID_ORGANISATIONS("lobid-orgs-index"), /***/
	GND("gnd-index");
	private String id; // NOPMD

	Index(final String name) {
		this.id = name;
	}

	/** @return The Elasticsearch index name. */
	public String id() { // NOPMD
		return id;
	}

	/**
	 * @param id The Elasticsearch index name
	 * @return The index enum element with the given id
	 * @throws IllegalArgumentException if there is no index with the id
	 */
	public static Index id(final String id) { // NOPMD
		for (Index index : values()) {
			if (index.id().equals(id)) {
				return index;
			}
		}
		throw new IllegalArgumentException("No such index: " + id);
	}
}