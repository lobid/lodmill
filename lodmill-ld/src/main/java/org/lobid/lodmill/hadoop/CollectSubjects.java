/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

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

	private static final String LOBID = "http://lobid.org/";
	private static final int REDUCERS = 1;
	private static final Logger LOG = LoggerFactory
			.getLogger(CollectSubjects.class);

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
		if (args.length != 2) {
			System.err.println("Usage: CollectSubjects <input path> <output path>");
			System.exit(-1);
		}
		conf.setStrings("mapred.textoutputformat.separator", " ");
		conf.setInt("mapred.tasktracker.reduce.tasks.maximum", REDUCERS);
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
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	/**
	 * Collect the object IDs to aquire details on a subject.
	 * 
	 * @author Fabian Steeg (fsteeg)
	 */
	static final class CollectSubjectsMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(final LongWritable key, final Text value,
				final Context context) throws IOException, InterruptedException {
			final String val = value.toString().trim();
			final Triple triple = asTriple(val);
			if (val.isEmpty() || triple == null
					|| !triple.getSubject().toString().startsWith(LOBID))
				return;
			final String subject =
					triple.getSubject().isBlank() ? val.substring(val.indexOf("_:"),
							val.indexOf(" ")).trim() : triple.getSubject().toString();
			if (triple.getObject().isBlank() || triple.getObject().isURI()) {
				final String object =
						triple.getObject().isBlank() ? val.substring(val.lastIndexOf("_:"),
								val.lastIndexOf(".")).trim() : triple.getObject().toString();
				LOG.info(String.format(
						"Collectiong ID found in object position (%s) of subject (%s)",
						object, subject));
				context.write(new Text(object), new Text(subject));
			} else
				context.write(new Text(subject), new Text(subject));
		}

		static Triple asTriple(final String val) {
			try {
				final Model model = ModelFactory.createDefaultModel();
				model.read(new StringReader(val), null, Format.N_TRIPLE.getName());
				return model.getGraph().find(Triple.ANY).next();
			} catch (com.hp.hpl.jena.shared.SyntaxError e) {
				LOG.warn(String.format("Could not parse triple '%s': %s, skipping",
						val, e.getMessage()));
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
			final Set<String> set = new HashSet<>();
			for (Text value : values)
				set.add(value.toString());
			context.write(key, new Text(Joiner.on(",").join(set)));
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
