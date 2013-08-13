/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.lobid.lodmill.JsonLdConverter;
import org.lobid.lodmill.JsonLdConverter.Format;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Convert RDF represented as N-Triples to JSON-LD for elasticsearch indexing.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class NTriplesToJsonLd implements Tool {

	private static final int NODES = 4; // e.g. 4 nodes in cluster
	private static final int SLOTS = 8; // e.g. 8 cores per node
	private static final String NEWLINE = "\n";
	static final String INDEX_NAME = "index.name";
	static final String INDEX_TYPE = "index.type";

	private static class NoSubjectCacheFilesFound extends RuntimeException {
		private static final long serialVersionUID = -5578567573519613713L;
	}

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

		static Map<String, Set<String>> subjects;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			final Path[] localCacheFiles =
					DistributedCache.getLocalCacheFiles(context.getConfiguration());
			subjects = new HashMap<>();
			if (localCacheFiles.length == 0)
				throw new NoSubjectCacheFilesFound();
			for (Path path : localCacheFiles)
				try (Scanner scanner = new Scanner(new File(path.toString()))) {
					while (scanner.hasNextLine()) {
						final String[] subjectAndValues = scanner.nextLine().split(" ");
						subjects.put(subjectAndValues[0].trim(),
								new HashSet<>(Arrays.asList(subjectAndValues[1].split(","))));
					}
				}
		}

		@Override
		public void map(final LongWritable key, final Text value,
				final Context context) throws IOException, InterruptedException {
			mapNonBlankNodesToTheirTriples(value, context, value.toString());
		}

		private static void mapNonBlankNodesToTheirTriples(final Text value,
				final Context context, final String val) throws IOException,
				InterruptedException {
			final Triple triple = asTriple(val);
			final String subject =
					triple.getSubject().isBlank() ? val.substring(val.indexOf("_:"),
							val.indexOf(" ")).trim() : triple.getSubject().toString();
			final Set<String> set = subjects.get(subject);
			if (set != null)
				for (String subj : set)
					context.write(new Text(wrapped(subj)), value);
		}

		private static String wrapped(final String string) {
			return "<" + string + ">";
		}

		private static Triple asTriple(final String val) {
			final Model model = ModelFactory.createDefaultModel();
			model.read(new StringReader(val), null, Format.N_TRIPLE.getName());
			return model.getGraph().find(Triple.ANY).next();
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
			final String triples = concatTriples(values);
			final String jsonLd =
					new JsonLdConverter(Format.N_TRIPLE).toJsonLd(triples);
			context.write(
					// write both with JSONValue for consistent escaping:
					new Text(JSONValue.toJSONString(createIndexMap(key, context))),
					new Text(JSONValue.toJSONString(JSONValue.parse(jsonLd))));
		}

		private static String concatTriples(final Iterable<Text> values) {
			final StringBuilder builder = new StringBuilder();
			for (Text value : values) {
				final String triple = fixInvalidUriLiterals(value);
				try {
					validate(triple);
					builder.append(triple).append(NEWLINE);
				} catch (Exception e) {
					System.err.println(String.format(
							"Could not read triple '%s': %s, skipping", triple,
							e.getMessage()));
					e.printStackTrace();
				}
			}
			return builder.toString();
		}

		private static void validate(final String val) {
			final Model model = ModelFactory.createDefaultModel();
			model.read(new StringReader(val), null, Format.N_TRIPLE.getName());
		}

		private static String fixInvalidUriLiterals(Text value) {
			return value.toString().replaceAll("\"\\s*?(http[s]?://[^\"]+)s*?\"",
					"<$1>");
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
