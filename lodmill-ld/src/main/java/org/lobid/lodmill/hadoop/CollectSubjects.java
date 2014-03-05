/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lobid.lodmill.JsonLdConverter.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Collect subjects required for details on lobid triples.
 * 
 * Maps lobid subjects URIs to subjects required to aquire details on the lobid
 * triples, i.e. URIs and blank nodes used in object position of triples with
 * lobid URIs in the subject position.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class CollectSubjects implements Tool {

	private static final int REDUCERS = 1;
	private static final Logger LOG = LoggerFactory
			.getLogger(CollectSubjects.class);
	static final Configuration MAP_FILE_CONFIG = new Configuration();
	static final String PREFIX_KEY = "target.subject.prefix";

	private static final Properties PROPERTIES = load();
	static final Set<String> TO_RESOLVE = props("resolve");
	static final Set<String> PREDICATES = props("predicates");
	static final Set<String> PARENTS = props("parents");

	private static Properties load() {
		final Properties props = new Properties();
		try {
			props.load(Thread.currentThread().getContextClassLoader()
					.getResourceAsStream("resolve.properties"));
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return props;
	}

	private static SortedSet<String> props(final String key) {
		return new TreeSet<>(Arrays.asList(PROPERTIES.getProperty(key).split(";")));
	}

	/**
	 * @param args Generic command-line arguments passed to {@link ToolRunner}.
	 */
	public static void main(final String[] args) {
		int res;
		try {
			res = ToolRunner.run(new CollectSubjects(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Configuration conf;

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			System.err
					.println("Usage: CollectSubjects <input path> <output path> <target subjects prefix> <index name>");
			System.exit(-1);
		}
		final String mapFileName = mapFileName(args[3]);
		conf.setStrings("mapred.textoutputformat.separator", " ");
		conf.setStrings("mapred.reduce.child.java.opts", "-Xmx4g");
		conf.setStrings("target.subject.prefix", args[2]);
		conf.setStrings("map.file.name", mapFileName);
		final Job job = new Job(conf);
		job.setNumReduceTasks(REDUCERS);
		job.setJarByClass(CollectSubjects.class);
		job.setJobName("CollectSubjects");
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(CollectSubjectsMapper.class);
		job.setReducerClass(CollectSubjectsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		asZippedMapFile(getFileSystem(conf), new Path(args[1] + "/part-r-00000"),
				new Path(args[1] + "/" + mapFileName + ".zip"), conf);
		System.exit(success ? 0 : 1);
		return 0;
	}

	static URI asZippedMapFile(final FileSystem fs,
			final Path subjectMappingsPath, final Path zipOutputLocation,
			final Configuration conf) throws IOException {
		writeToMapFile(subjectMappingsPath, fs, conf);
		final Path zippedMapFilePath = zipMapFile(fs, zipOutputLocation, conf);
		return zippedMapFilePath.toUri();
	}

	private static void writeToMapFile(final Path subjectMappingsPath,
			final FileSystem fs, final Configuration conf) throws IOException {
		try (final MapFile.Writer writer =
				new MapFile.Writer(MAP_FILE_CONFIG, fs, conf.get("map.file.name"),
						Text.class, Text.class);
				final InputStream inputStream = fs.open(subjectMappingsPath);
				final Scanner scanner = new Scanner(inputStream)) {
			while (scanner.hasNextLine()) {
				final String[] subjectAndValues = scanner.nextLine().split(" ");
				writer.append(new Text(subjectAndValues[0].trim()), new Text(
						subjectAndValues[1].trim()));
			}
		}
	}

	private static Path zipMapFile(final FileSystem fs,
			final Path zipOutputLocation, final Configuration conf)
			throws IOException, FileNotFoundException {
		final Path[] outputFiles =
				FileUtil.stat2Paths(fs.listStatus(new Path(conf.get("map.file.name")),
						new Utils.OutputFileUtils.OutputFilesFilter()));
		try (final FSDataOutputStream fos = fs.create(zipOutputLocation);
				final ZipOutputStream zos = new ZipOutputStream(fos)) {
			add(zos, new ZipEntry("data"), fs.open(outputFiles[0]));
			add(zos, new ZipEntry("index"), fs.open(outputFiles[1]));
		}
		return zipOutputLocation;
	}

	private static void add(final ZipOutputStream zos, final ZipEntry data,
			final InputStream in) throws IOException, FileNotFoundException {
		zos.putNextEntry(data);
		IOUtils.copyBytes(in, zos, 1024);
		zos.closeEntry();
	}

	static FileSystem getFileSystem(final Configuration conf) throws IOException {
		return FileSystem.get(URI.create(conf.get("map.file.name")),
				MAP_FILE_CONFIG);
	}

	/**
	 * Collect the object IDs to aquire details on a subject.
	 * 
	 * @author Fabian Steeg (fsteeg)
	 */
	static final class CollectSubjectsMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private static final String DEWEY = "http://dewey.info/class";
		private static final String DEWEY_SUFFIX = "2009/08/about.en";

		private String prefix;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			prefix = context.getConfiguration().get(PREFIX_KEY);
		}

		@Override
		public void map(final LongWritable key, final Text value,
				final Context context) throws IOException, InterruptedException {
			final String val = value.toString().trim();
			if (val.isEmpty())
				return;
			final Triple triple = asTriple(val);
			if (shouldProcess(triple)) {
				final String subject = getSubject(val, triple, context.getInputSplit());
				final String object =
						preprocess(getObject(val, triple, context.getInputSplit()));
				LOG.info(String.format(
						"Collectiong ID found in object position (%s) of subject (%s)",
						object, subject));
				context.write(new Text(object), new Text(subject));
			}
		}

		private boolean shouldProcess(final Triple triple) {
			return triple != null
					&& triple.getSubject().isURI()
					&& triple.getSubject().toString()
							.startsWith(prefix == null ? "" : prefix)
					&& TO_RESOLVE.contains(triple.getPredicate().toString())
					&& (triple.getObject().isBlank() || triple.getObject().isURI());
		}

		private static String getSubject(final String val, final Triple triple,
				final InputSplit inputSplit) {
			return triple.getSubject().isBlank() ? blankSubjectLabel(val, inputSplit)
					: triple.getSubject().toString();
		}

		private static String getObject(final String val, final Triple triple,
				final InputSplit inputSplit) {
			return triple.getObject().isBlank() ? blankObjectLabel(val, inputSplit)
					: triple.getObject().toString();
		}

		private static String preprocess(final String object) {
			return object.contains(DEWEY) ? object + DEWEY_SUFFIX : object;
		}

		static Triple asTriple(final String val) {
			try {
				final Model model = ModelFactory.createDefaultModel();
				model.read(new StringReader(val), null, Format.N_TRIPLE.getName());
				return model.getGraph().find(Triple.ANY).next();
			} catch (com.hp.hpl.jena.shared.SyntaxError e) {
				LOG.warn(String.format("Could not parse triple '%s': %s, skipping",
						val, e.getMessage()));
			} catch (java.util.NoSuchElementException e1) {
				LOG.warn(String.format("No triple '%s': %s, skipping", val,
						e1.getMessage()));
			}
			return null;
		}
	}

	/**
	 * Join the subjects required for details under the main subject.
	 * 
	 * @author Fabian Steeg (fsteeg)
	 */
	static final class CollectSubjectsReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(final Text key, final Iterable<Text> values,
				final Context context) throws IOException, InterruptedException {
			context.write(key, new Text(Joiner.on(",").join(values)));
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

	static String blankSubjectLabel(final String val, final InputSplit inputSplit) {
		return val.substring(val.indexOf("_:"), val.indexOf(" ")).trim()
				+ createBlankNodeSuffix(inputSplit);
	}

	static String blankObjectLabel(final String val, final InputSplit inputSplit) {
		return val.substring(val.lastIndexOf("_:"), val.lastIndexOf(".")).trim()
				+ createBlankNodeSuffix(inputSplit);
	}

	private static String createBlankNodeSuffix(final InputSplit inputSplit) {
		return ":"
				+ ((FileSplit) inputSplit).getPath().toUri().getPath()
						.replaceAll("/user/[^/]+/", "");
	}

	/**
	 * @param prefix The prefix to use for making the map file name unique
	 * @return A file name for the map file, with the given prefix
	 */
	public static String mapFileName(String prefix) {
		return prefix + "subjects.map";
	}
}
