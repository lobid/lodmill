package hadoop.sample.lobid;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.culturegraph.semanticweb.sink.AbstractModelWriter.Format;
import org.json.simple.JSONValue;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import de.dfki.km.json.JSONUtils;
import de.dfki.km.json.jsonld.impl.JenaJSONLDSerializer;

public class LobidToJsonLdReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public final void reduce(final Text key, final Iterable<Text> values,
			final Context context) throws IOException, InterruptedException {
		final StringBuilder builder = new StringBuilder();
		// Concat all triples for the subject:
		for (Text value : values) {
			builder.append(value.toString()).append("\n");
		}
		// Load them into a model:
		final Model model = ModelFactory.createDefaultModel();
		model.read(new StringReader(builder.toString()), null,
				Format.N_TRIPLE.getName());
		// And convert the model to JSON-LD:
		final JenaJSONLDSerializer serializer = new JenaJSONLDSerializer();
		serializer.importModel(model);
		final String resource = JSONUtils.toString(serializer.asObject());
		context.write(
				// write key and value with JSONValue for consistent escaping:
				new Text(JSONValue.toJSONString(createIndexMap(key))),
				new Text(JSONValue.toJSONString(JSONValue.parse(resource))));
	}

	private Map<String, Map<?, ?>> createIndexMap(final Text key) {
		final Map<String, String> map = new HashMap<String, String>();
		map.put("_index", "lobid-json-ld");
		map.put("_type", "lobid-resource");
		map.put("_id", key.toString().substring(1, key.getLength() - 1));
		final Map<String, Map<?, ?>> index = new HashMap<String, Map<?, ?>>();
		index.put("index", map);
		return index;
	}
}