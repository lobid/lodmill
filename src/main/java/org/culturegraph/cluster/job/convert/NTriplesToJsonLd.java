package org.culturegraph.cluster.job.convert;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.culturegraph.cluster.util.AbstractJobLauncher;
import org.culturegraph.cluster.util.ConfigConst;
import org.culturegraph.semanticweb.sink.AbstractModelWriter.Format;
import org.json.simple.JSONValue;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import de.dfki.km.json.JSONUtils;
import de.dfki.km.json.jsonld.impl.JenaJSONLDSerializer;

/**
 * Convert RDF represented as N-Triples to JSON-LD for elasticsearch indexing.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class NTriplesToJsonLd extends AbstractJobLauncher {

	private static final int NODES = 4; // e.g. 4 nodes in cluster
	private static final int SLOTS = 2; // e.g. 2 cores per node
	private static final String NEWLINE = "\n";

	public static void main(final String[] args) {
		launch(new NTriplesToJsonLd(), args);
	}

	@Override
	protected final Configuration prepareConf(final Configuration conf) {
		addRequiredArguments(ConfigConst.INPUT_PATH, ConfigConst.OUTPUT_PATH);
		conf.setStrings("mapred.textoutputformat.separator", NEWLINE);
		return getConf();
	}

	@Override
	protected final void configureJob(final Job job, final Configuration conf)
			throws IOException {
		configureFileInputMapper(job, conf, NTriplesToJsonLdMapper.class,
				Text.class, Text.class);
		job.setNumReduceTasks(NODES * SLOTS);
		configureTextOutputReducer(job, conf, NTriplesToJsonLdReducer.class);
	}

	/**
	 * Map (non-blank) subject URIs of N-Triples to the triples.
	 * 
	 * @author Fabian Steeg (fsteeg)
	 */
	static final class NTriplesToJsonLdMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(final LongWritable key, final Text value,
				final Context context) throws IOException, InterruptedException {
			final String val = value.toString();
			// we take all non-blank nodes and map them to their triples:
			if (val.startsWith("<http")) {
				final String subject = val.substring(0, val.indexOf('>') + 1);
				context.write(new Text(subject), value);
			}
		}
	}

	/**
	 * Reduce all N-Triples with a common subject to a JSON-LD representation.
	 * 
	 * @author Fabian Steeg (fsteeg)
	 */
	static final class NTriplesToJsonLdReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(final Text key, final Iterable<Text> values,
				final Context context) throws IOException, InterruptedException {
			// Load them into a model:
			final Model model = ModelFactory.createDefaultModel();
			for (Text value : values) {
				try {
					/* Convert literal URIs in N-Triple to real URIs: */
					final String triple =
							value.toString().replaceAll(
									"\"\\s*?(http[s]?://[^\"]+)s*?\"", "<$1>");
					model.read(new StringReader(triple), null,
							Format.N_TRIPLE.getName());
				} catch (Exception e) {
					System.err.println(String.format("Could not read '%s': %s",
							value, e.getMessage()));
				}
			}
			// And convert the model to JSON-LD:
			final JenaJSONLDSerializer serializer = new JenaJSONLDSerializer();
			serializer.importModel(model);
			final String resource = JSONUtils.toString(serializer.asObject());
			context.write(
					// write both with JSONValue for consistent escaping:
					new Text(JSONValue.toJSONString(createIndexMap(key))),
					new Text(JSONValue.toJSONString(JSONValue.parse(resource))));
		}

		private Map<String, Map<?, ?>> createIndexMap(final Text key) {
			final Map<String, String> map = new HashMap<String, String>();
			map.put("_index", "json-ld-index");
			map.put("_type", "json-ld");
			map.put("_id", key.toString().substring(1, key.getLength() - 1));
			final Map<String, Map<?, ?>> index =
					new HashMap<String, Map<?, ?>>();
			index.put("index", map);
			return index;
		}
	}
}
