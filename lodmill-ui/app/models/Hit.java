/* Copyright 2013 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package models;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.elasticsearch.search.SearchHit;

import play.Logger;
import play.libs.Json;

import com.fasterxml.jackson.databind.JsonNode;

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
				final JsonNode birth = findNestedValue(json, fields.get(1));
				final JsonNode death = findNestedValue(json, fields.get(2));
				if (birth == null)
					document.matchedField = field.toString();
				else {
					final String format =
							String.format("%s (%s-%s)", field.toString(), birth.asText(),
									death == null ? "" : death.asText());
					document.matchedField = format;
				}
			} else
				document.matchedField = field.toString();
			return document;
		}

		private JsonNode findNestedValue(final JsonNode json, final String fieldName) {
			final String stripped =
					fieldName.replace(GRAPH_KEY + ".", "").replace("." + VALUE_KEY, "");
			final JsonNode element = json.findValue(stripped);
			return element == null ? null : element.findValue("@value");
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
	private static final String VALUE_KEY = "@value";
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

	private static String firstExisting() {
		for (String currentField : fields) {
			final String searchField = currentField /*@formatter:off*/
					.replace(GRAPH_KEY + ".", "")
					.replace("." + ID_KEY, "")
					.replace("." + VALUE_KEY, ""); /*@formatter:on*/
			final JsonNode value =
					Json.toJson(hit.getSource()).findValue(searchField);
			if (value != null) {
				final JsonNode nestedId = value.findValue(ID_KEY);
				if (nestedId != null)
					return nestedId.asText();
				final JsonNode nestedValue = value.findValue(VALUE_KEY);
				if (nestedValue != null)
					return nestedValue.asText();
				if (!value.asText().trim().isEmpty())
					return value.asText();
			}
		}
		if (fields.contains("_all"))
			return hit.getId();
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
