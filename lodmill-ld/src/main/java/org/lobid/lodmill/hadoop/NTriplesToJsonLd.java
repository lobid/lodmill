/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONValue;
import org.lobid.lodmill.JsonLdConverter.Format;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import de.dfki.km.json.JSONUtils;
import de.dfki.km.json.jsonld.impl.JenaJSONLDSerializer;

/**
 * Convert RDF represented as N-Triples to JSON-LD for elasticsearch indexing.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class NTriplesToJsonLd implements Tool {

	private static final int NODES = 4; // e.g. 4 nodes in cluster
	private static final int SLOTS = 2; // e.g. 2 cores per node
	private static final String NEWLINE = "\n";
	static final String INDEX_NAME = "index.name";
	static final String INDEX_TYPE = "index.type";

	/**
	 * @param args Generic command-line arguments passed to {@link ToolRunner}.
	 */
	public static void main(final String[] args) {
		try {
			int res = ToolRunner.run(new NTriplesToJsonLd(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Configuration conf;

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Usage: NTriplesToJsonLd"
					+ " <input path> <output path> <index name> <index type>");
			System.exit(-1);
		}
		conf = getConf();
		conf.setStrings("mapred.textoutputformat.separator", NEWLINE);
		conf.setInt("mapred.tasktracker.reduce.tasks.maximum", SLOTS);
		conf.set(INDEX_NAME, args[2]);
		conf.set(INDEX_TYPE, args[3]);
		final Job job = new Job(conf);
		job.setNumReduceTasks(NODES * SLOTS);
		job.setJarByClass(NTriplesToJsonLd.class);
		job.setJobName("LobidToJsonLd");
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(NTriplesToJsonLdMapper.class);
		job.setReducerClass(NTriplesToJsonLdReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
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
							value.toString().replaceAll("\"\\s*?(http[s]?://[^\"]+)s*?\"",
									"<$1>");
					model.read(new StringReader(triple), null, Format.N_TRIPLE.getName());
				} catch (Exception e) {
					System.err.println(String.format("Could not read '%s': %s", value,
							e.getMessage()));
				}
			}
			// And convert the model to JSON-LD:
			final JenaJSONLDSerializer serializer = new JenaJSONLDSerializer();
			serializer.importModel(model);
			final String resource = JSONUtils.toString(serializer.asObject());
			context.write(
					// write both with JSONValue for consistent escaping:
					new Text(JSONValue.toJSONString(createIndexMap(key, context))),
					new Text(JSONValue.toJSONString(JSONValue.parse(resource))));
		}

		private static Map<String, Map<?, ?>> createIndexMap(final Text key,
				final Context context) {
			final Map<String, String> map = new HashMap<>();
			map.put("_index", context.getConfiguration().get(INDEX_NAME));
			map.put("_type", context.getConfiguration().get(INDEX_TYPE));
			map.put("_id", key.toString().substring(1, key.getLength() - 1));
			final Map<String, Map<?, ?>> index = new HashMap<>();
			index.put("index", map);
			return index;
		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;

	}

}
