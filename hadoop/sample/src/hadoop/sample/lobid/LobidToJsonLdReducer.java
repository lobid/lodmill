package hadoop.sample.lobid;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.culturegraph.semanticweb.sink.AbstractModelWriter.Format;

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
		final Object output = serializer.asObject();
		context.write(key, new Text(JSONUtils.toString(output)));
	}
}