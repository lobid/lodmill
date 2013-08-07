/* Copyright 2012-2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;

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

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import fr.inria.jfresnel.fsl.FSLHierarchyStore;
import fr.inria.jfresnel.fsl.FSLNSResolver;
import fr.inria.jfresnel.fsl.FSLPath;
import fr.inria.jfresnel.fsl.jena.FSLJenaEvaluator;

/**
 * Resolve URIs in the object position of lobid resources.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class ResolveObjectUrisInLobidNTriples implements Tool {

	private static final Properties PROPERTIES = load();
	private static final Set<String> TO_RESOLVE = props("resolve");
	static final Set<String> PREDICATES = props("predicates");
	static final Set<String> FSL_PATHS = props("fsl-paths");
	private static final String LOBID = "http://lobid.org/";
	private static final String DEWEY = "http://dewey.info/class";
	private static final String DEWEY_SUFFIX = "2009/08/about.en";

	private static final int NODES = 4; // e.g. 4 nodes in cluster
	private static final int SLOTS = 8; // e.g. 8 cores per node
	private static final String NEWLINE = "\n";

	private static final Logger LOG = LoggerFactory
			.getLogger(ResolveObjectUrisInLobidNTriples.class);

	/**
	 * @param args Generic command-line arguments passed to {@link ToolRunner}.
	 */
	public static void main(final String[] args) {
		int res;
		try {
			res = ToolRunner.run(new ResolveObjectUrisInLobidNTriples(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

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

	private Configuration conf;

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Usage: ResolveObjectUrisInLobidNTriples <input path> <output path>");
			System.exit(-1);
		}
		conf.setStrings("mapred.textoutputformat.separator", "");
		conf.setInt("mapred.tasktracker.reduce.tasks.maximum", SLOTS);
		final Job job = new Job(conf);
		job.setNumReduceTasks(NODES * SLOTS);
		job.setJarByClass(ResolveObjectUrisInLobidNTriples.class);
		job.setJobName("ResolveObjectUrisInLobidNTriples");
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(ResolveTriplesMapper.class);
		job.setReducerClass(ResolveTriplesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	/**
	 * Collect (non-blank) lobid triples and triples required for resolving lobid
	 * triples, using the resolution ID as a key.
	 * 
	 * @author Fabian Steeg (fsteeg)
	 */
	static final class ResolveTriplesMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(final LongWritable key, final Text value,
				final Context context) throws IOException, InterruptedException {
			final String val = value.toString();
			if (val.trim().isEmpty()) {
				return; /* Skip empty lines */
			}
			/* Process lobid triples and triples needed to resolve lobid triples: */
			if (val.substring(1).startsWith(LOBID) || exists(val, PREDICATES)) {
				/*
				 * We always group under the resolution ID key: for triples to be
				 * resolved, that's the object (i.e. the entity to be resolved), for
				 * others (i.e. entities providing resolution information), it's the
				 * subject:
				 */
				final Triple triple = asTriple(val);
				final String newKey =
						exists(val, TO_RESOLVE) ? resolvable(triple.getObject().toString())
								: triple.getSubject().toString();
				context.write(new Text(newKey), preprocess(val));
			}
		}

		private static Triple asTriple(final String val) {
			final Model model = ModelFactory.createDefaultModel();
			model.read(new StringReader(val), null, Format.N_TRIPLE.getName());
			return model.getGraph().find(Triple.ANY).next();
		}

		private static boolean exists(final String val, final Set<String> vals) {
			return Sets.filter(vals, new Predicate<String>() {
				@Override
				public boolean apply(final String string) {
					return val.contains(string);
				}
			}).size() > 0;
		}

		/*
		 * These methods make sure that we can resolve the triples we are using. For
		 * Dewey triples, this requires changing the format to conform to something
		 * in the subject position in the Dewey data, e.g. we change
		 * <http://dewey.info/class/325> -->
		 * <http://dewey.info/class/325/2009/08/about.en>
		 * 
		 * This way, what we have in object position (what we want to resolve) is
		 * identical to a subject in the Dewey data, and we can thus find an FSL
		 * path like 'o/dct:subject/o/skos:prefLabel/text()'. See resolve.properties
		 * for actual paths (replaced * with o for Javadoc).
		 */

		private static Text preprocess(final String triple) {
			if (triple.contains(DEWEY)) {
				final String obj = asTriple(triple).getObject().toString();
				return new Text(triple.replace(obj, resolvable(obj)));
			}
			return new Text(triple);
		}

		private static String resolvable(final String uri) {
			return uri.contains(DEWEY) ? uri + DEWEY_SUFFIX : uri;
		}
	}

	/**
	 * Load all triples related to a resolving entity into a model and resolve.
	 * 
	 * @author Fabian Steeg (fsteeg)
	 */
	static final class ResolveTriplesReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(final Text key, final Iterable<Text> values,
				final Context context) throws IOException, InterruptedException {
			final StringBuilder builder = new StringBuilder();
			// Concat all triples for the subject:
			for (Text value : values) {
				builder.append(value.toString()).append(NEWLINE);
			}
			// Load them into a model:
			Model model = ModelFactory.createDefaultModel();
			model.read(new StringReader(builder.toString()), null,
					Format.N_TRIPLE.getName());
			// Resolve and write results:
			model = resolveUrisToLiterals(model);
			writeResolvedLobidTriples(context, model);
		}

		private static void writeResolvedLobidTriples(final Context context,
				final Model model) throws IOException, InterruptedException {
			for (Triple triple : model.getGraph().find(Triple.ANY).toList()) {
				if (triple.getSubject().toString().startsWith(LOBID)) {
					final String objString = triple.getObject().toString();
					final String objResult =
							triple.getObject().isURI() ? wrapped(objString) : objString;
					final Text predAndObj =
							new Text(wrapped(triple.getPredicate().toString()) + " "
									+ objResult + " .");
					context.write(new Text(wrapped(triple.getSubject().toString())),
							predAndObj);
				}
			}
		}

		private static String wrapped(final String string) {
			return "<" + string + ">";
		}

		@SuppressWarnings("serial")
		/* data for creating new triples... */
		// @formatter:off
		final private Map<String, String[]> map =
				new HashMap<String, String[]>() {{ // NOPMD
				/* 
				 * ...e.g.: for an FSL path that resolves 'dc:creator',
				 * create a new predicate by concatenating
				 * 'http://purl.org/dc/elements/1.1/creator#' and the thing
				 * that follows 'gnd:' in the path - e.g.
				 * 'http://purl.org/dc/elements/1.1/creator#dateOfBirth'
				 */
				put("dc:creator", new String[] {
						"http://purl.org/dc/elements/1.1/creator#", "gnd:" });
				put("dct:subject", new String[] {
						"http://purl.org/dc/terms/subject#", "skos:" });
				put("geo:location", new String[] {
						"http://www.w3.org/2003/01/geo/wgs84_pos#", "geo:" });
		}};

		private Model resolveUrisToLiterals(final Model model) {
			final FSLNSResolver nsr = new FSLNSResolver();
			nsr.addPrefixBinding("dc", "http://purl.org/dc/elements/1.1/");
			nsr.addPrefixBinding("dct", "http://purl.org/dc/terms/");
			nsr.addPrefixBinding("gnd", "http://d-nb.info/standards/elementset/gnd#");
			nsr.addPrefixBinding("skos", "http://www.w3.org/2004/02/skos/core#");
			nsr.addPrefixBinding("geo", "http://www.w3.org/2003/01/geo/wgs84_pos#");
			// @formatter:on
			final FSLHierarchyStore fhs = new FSLHierarchyStore();
			final FSLJenaEvaluator fje = new FSLJenaEvaluator(nsr, fhs);
			fje.setModel(model);
			for (String fslPath : FSL_PATHS) {
				final FSLPath path =
						FSLPath.pathFactory(fslPath, nsr, FSLPath.NODE_STEP);
				for (String p : map.keySet()) {
					if (fslPath.contains(p)) {
						addResolvedTriples(model, fje, path,
								newPred(fslPath, map.get(p)[0], map.get(p)[1]));
						break; // done with the current path
					}
				}
			}
			return model;
		}

		private static String newPred(final String fslPath, final String prefix,
				final String namespace) {
			return prefix
					+ fslPath.substring(
							fslPath.lastIndexOf(namespace) + namespace.length(),
							fslPath.lastIndexOf('/'));
		}

		private static Model addResolvedTriples(final Model model,
				final FSLJenaEvaluator fje, final FSLPath path,
				final String newPredicate) {
			final Vector<Vector<Object>> pathInstances = fje.evaluatePath(path);
			for (Vector<Object> object : pathInstances) {
				final Triple resolved =
						Triple.create(Node.createURI(object.firstElement().toString()),
								Node.createURI(newPredicate),
								Node.createLiteral(object.lastElement().toString()));
				model.getGraph().add(resolved);
			}
			return model;
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
