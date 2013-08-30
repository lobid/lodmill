/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.elasticsearch.search.SearchHit;

import play.Logger;
import play.libs.Json;

/**
 * Process different kinds of result hits.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public enum Hit {
	/***/
	LIST(List.class) {
		@Override
		Document process(final String query, final Document document) {
			final List<?> list = (List<?>) field;
			if (list.get(0) instanceof String) {
				STRING.process(query, document);
			} else if (list.get(0) instanceof Map) {
				@SuppressWarnings("unchecked")
				final List<Map<String, Object>> maps =
						(List<Map<String, Object>>) field;
				processMaps(query, document, maps);
			}
			return document;
		}
	},
	/***/
	STRING(String.class) {
		@Override
		Document process(final String query, final Document document) {
			if (fields.get(0).contains("preferredNameForThePerson")) {
				final JsonNode json = Json.toJson(hit.getSource());
				final JsonNode birth = json.findValue(stripGraphPrefix(fields.get(1)));
				final JsonNode death = json.findValue(stripGraphPrefix(fields.get(2)));
				if (birth == null) {
					document.matchedField = field.toString();
				} else {
					final String format =
							String.format("%s (%s-%s)", field.toString(), birth.asText(),
									death == null ? "" : death.asText());
					document.matchedField = format;
				}
			} else {
				document.matchedField = field.toString();
			}
			return document;
		}

		private String stripGraphPrefix(final String fieldString) {
			return fieldString.replace(GRAPH_KEY + ".", "");
		}
	},
	/***/
	MAP(Map.class) {
		@Override
		Document process(final String query, final Document document) {
			@SuppressWarnings("unchecked")
			final Map<String, Object> map = (Map<String, Object>) field;
			processMaps(query, document, Arrays.asList(map));
			return document;
		}
	};

	private static Object field;
	private static List<String> fields;
	private static SearchHit hit;
	private final Class<?> fieldType; // NOPMD
	private static final String GRAPH_KEY = "@graph";
	private static final String ID_KEY = "@id";

	static Hit of(final SearchHit searchHit, final List<String> searchFields) { // NOPMD
		hit = searchHit;
		fields = searchFields;
		field = firstExisting();
		for (Hit hitElement : values()) {
			if (hitElement.fieldType.isInstance(field)) {
				return hitElement;
			}
		}
		throw new IllegalArgumentException("No hit type for field: " + field);
	}

	private static Object firstExisting() {
		for (String currentField : fields) {
			final String searchField =
					(currentField.contains(GRAPH_KEY) ? currentField.replace(GRAPH_KEY
							+ ".", "") : currentField).replace("." + ID_KEY, "");
			final JsonNode value =
					Json.toJson(hit.getSource()).findValue(searchField);
			if (value != null) {
				final JsonNode nestedValue = value.findValue(ID_KEY);
				return nestedValue != null ? nestedValue.asText() : value.asText();
			}
		}
		Logger.warn(String.format("Hit '%s' contains none of the fields: '%s'",
				hit.getSource(), fields));
		return null;
	}

	private static void processMaps(final String query, final Document document,
			final List<Map<String, Object>> maps) {
		for (Map<String, Object> map : maps) {
			if (map.get(ID_KEY).toString().contains(query)) {
				document.matchedField = map.get(ID_KEY).toString();
				break;
			}
		}
	}

	Hit(final Class<?> fieldType) {
		this.fieldType = fieldType;
	}

	abstract Document process(String query, Document document);
}
