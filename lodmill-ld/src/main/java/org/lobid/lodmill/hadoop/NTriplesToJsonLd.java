/* Copyright 2012-2014 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
import org.lobid.lodmill.hadoop.CollectSubjects.CollectSubjectsMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
	private static final Logger LOG = LoggerFactory
			.getLogger(NTriplesToJsonLd.class);
	private Configuration conf;

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

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 6) {
			System.err
					.println("Usage: NTriplesToJsonLd"
							+ " <input path> <subjects path> <output path> <index name> <index type> <target subjects prefix>");
			System.exit(-1);
		}
		conf = getConf();
		final String indexName = args[3];
		final String mapFileName = CollectSubjects.mapFileName(indexName);
		conf.setStrings("mapred.textoutputformat.separator", NEWLINE);
		conf.setStrings("target.subject.prefix", args[5]);
		conf.setStrings("map.file.name", mapFileName);
		conf.set(INDEX_NAME, indexName);
		conf.set(INDEX_TYPE, args[4]);
		final Job job = Job.getInstance(conf);
		job.addCacheFile(new Path(args[1] + "/" + mapFileName + ".zip").toUri());
		job.setNumReduceTasks(NODES * SLOTS);
		job.setJarByClass(NTriplesToJsonLd.class);
		job.setJobName("LobidToJsonLd");
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setMapperClass(NTriplesToJsonLdMapper.class);
		job.setReducerClass(NTriplesToJsonLdReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	/**
	 * Map subject URIs of N-Triples to the triples.
	 * 
	 * @author Fabian Steeg (fsteeg)
	 */
	static final class NTriplesToJsonLdMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private Reader reader;
		private String prefix;
		private Set<String> predicates;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			prefix = context.getConfiguration().get(CollectSubjects.PREFIX_KEY);
			predicates = CollectSubjects.PREDICATES;
			final String rawMapFile = context.getConfiguration().get("map.file.name");
			final URI zippedMapFile = findZip(context.getCacheFiles(), rawMapFile);
			if (zippedMapFile != null)
				initMapFileReader(new Path(zippedMapFile), new Path(rawMapFile),
						context.getConfiguration());
			else
				LOG.warn("No subjects cache files found!");
		}

		private static URI findZip(final URI[] localCacheFiles,
				final String rawMapFileName) {
			if (localCacheFiles == null || rawMapFileName == null)
				return null;
			for (URI uri : localCacheFiles)
				if (uri.toString().contains(rawMapFileName))
					return uri;
			return null;
		}

		private void initMapFileReader(final Path zipFile, final Path outputPath,
				final Configuration config) throws IOException, FileNotFoundException {
			LOG.info("Unzipping map file at %s to %s", zipFile, outputPath);
			unzip(zipFile, outputPath, config);
			reader = new MapFile.Reader(outputPath, CollectSubjects.MAP_FILE_CONFIG);
			if (reader == null)
				throw new IllegalStateException(String.format(
						"Could not load map file data from %s", outputPath));
		}

		private static void unzip(final Path zipFile, final Path output,
				final Configuration config) throws FileNotFoundException, IOException {
			try (final ZipInputStream zis =
					new ZipInputStream(FileSystem.get(config).open(zipFile))) {
				for (ZipEntry ze; (ze = zis.getNextEntry()) != null; zis.closeEntry()) {
					try (final OutputStream fos =
							FileSystem.get(CollectSubjects.MAP_FILE_CONFIG).create(
									new Path(output + "/" + ze.getName()))) {
						IOUtils.copyBytes(zis, fos, 1024);
					}
				}
			}
		}

		@Override
		public void map(final LongWritable key, final Text value,
				final Context context) throws IOException, InterruptedException {
			final String val = value.toString().trim();
			if (val.isEmpty())
				return;
			final Triple triple = CollectSubjectsMapper.asTriple(val);
			if (triple != null)
				mapSubjectsToTheirTriples(value, context, val, triple);
		}

		private void mapSubjectsToTheirTriples(final Text value,
				final Context context, final String val, final Triple triple)
				throws IOException, InterruptedException {
			final String subject =
					triple.getSubject().isBlank() ? CollectSubjects.blankSubjectLabel(
							val, context.getInputSplit()) : triple.getSubject().toString();
			if (subjectIsUriToBeCollected(triple)
					&& !objectIsUnresolvedBlankNode(triple))
				context.write(new Text(wrapped(subject.trim())), value);
			if (predicates.contains(triple.getPredicate().toString())
					&& reader != null)
				writeAdditionalSubjects(subject, value, context);
		}

		private boolean subjectIsUriToBeCollected(final Triple triple) {
			return triple.getSubject().isURI()
					&& triple.getSubject().toString()
							.startsWith(prefix == null ? "" : prefix);
		}

		private static boolean objectIsUnresolvedBlankNode(final Triple triple) {
			return triple.getObject().isBlank()
					&& !CollectSubjects.TO_RESOLVE.contains(triple.getPredicate()
							.toString());
		}

		private void writeAdditionalSubjects(final String subject,
				final Text value, final Context context) throws IOException,
				InterruptedException {
			final Writable res = reader.get(new Text(subject), new Text());
			if (res != null) {
				for (String subj : res.toString().split(","))
					context.write(new Text(wrapped(subj.trim())), value);
			}
		}

		private static String wrapped(final String string) {
			return "<" + string + ">";
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
			final JsonNode node =
					new ObjectMapper().readValue(jsonLd, JsonNode.class);
			final JsonNode parent =
					node.findValue(CollectSubjects.PARENTS.iterator().next());
			context.write(
					// write both with JSONValue for consistent escaping:
					new Text(JSONValue.toJSONString(createIndexMap(key, context,
							parent != null ? parent.findValue("@id").asText() : null))),
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
					LOG.error(String.format("Could not read triple '%s': %s, skipping",
							triple, e.getMessage()), e);
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
				final Context context, final String parent) {
			final Map<String, String> map = new HashMap<>();
			map.put("_index", context.getConfiguration().get(INDEX_NAME));
			map.put("_type", context.getConfiguration().get(INDEX_TYPE));
			map.put("_id", key.toString().substring(1, key.toString().length() - 1));
			if (context.getConfiguration().get(INDEX_TYPE)
					.equals("json-ld-lobid-item"))
				map.put("_parent", parent != null ? parent : "none");
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
