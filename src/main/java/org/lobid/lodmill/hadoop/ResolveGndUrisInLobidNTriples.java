/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.culturegraph.cluster.util.AbstractJobLauncher;
import org.culturegraph.cluster.util.ConfigConst;
import org.culturegraph.semanticweb.sink.AbstractModelWriter.Format;

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
 * Resolve GND URIs in the object position of lobid resources.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public class ResolveGndUrisInLobidNTriples extends AbstractJobLauncher {

	private static final Properties PROPERTIES = load();
	public static final Set<String> PREDICATES = props("predicates");
	public static final Set<String> FSL_PATHS = props("fsl-paths");

	private static final int NODES = 4; // e.g. 4 nodes in cluster
	private static final int SLOTS = 2; // e.g. 2 cores per node
	private static final String NEWLINE = "\n";
	private static final String LOBID_RESOURCE = "http://lobid.org/resource/";

	public static void main(final String[] args) {
		launch(new ResolveGndUrisInLobidNTriples(), args);
	}

	private static Properties load() {
		Properties p = new Properties();
		try {
			p.load(Thread.currentThread().getContextClassLoader()
					.getResourceAsStream("resolve.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return p;
	}

	private static SortedSet<String> props(String key) {
		return new TreeSet<String>(Arrays.asList(PROPERTIES.getProperty(key)
				.split(";")));
	}

	@Override
	protected final Configuration prepareConf(final Configuration conf) {
		addRequiredArguments(ConfigConst.INPUT_PATH, ConfigConst.OUTPUT_PATH);
		conf.setStrings("mapred.textoutputformat.separator", "");
		return getConf();
	}

	@Override
	protected final void configureJob(final Job job, final Configuration conf)
			throws IOException {
		configureFileInputMapper(job, conf, ResolveTriplesMapper.class,
				Text.class, Text.class);
		job.setNumReduceTasks(NODES * SLOTS);
		configureTextOutputReducer(job, conf, ResolveTriplesReducer.class);
	}

	/**
	 * Collect (non-blank) GND and lobid triples under GND ID keys.
	 * 
	 * @author Fabian Steeg (fsteeg)
	 */
	static final class ResolveTriplesMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private static final String CREATOR =
				"http://purl.org/dc/elements/1.1/creator";

		@Override
		public void map(final LongWritable key, final Text value,
				final Context context) throws IOException, InterruptedException {
			final String val = value.toString();
			// we take non-blank nodes and map them to their triples:
			if (val.startsWith("<http")
					&& (neededForResolving(val) || val.substring(1).startsWith(
							LOBID_RESOURCE))) {
				context.write(new Text(
				/*
				 * We always group under the GND ID key: for creator triples,
				 * that's the object, for preferredName triples, the subject:
				 */
				val.contains(CREATOR) ? objUri(val) : subjUri(val)), value);
			}
		}

		private boolean neededForResolving(final String val) {
			return Sets.filter(PREDICATES, new Predicate<String>() {
				public boolean apply(String string) {
					return val.contains(string);
				}
			}).size() > 0;
		}

		private String subjUri(final String val) {
			return val.substring(0, val.indexOf('>') + 1);
		}

		private String objUri(final String val) {
			return val
					.substring(val.lastIndexOf('<'), val.lastIndexOf('>') + 1);
		}
	}

	/**
	 * Load all triples related to a GND ID into a model and resolve URIs.
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

		private void writeResolvedLobidTriples(final Context context,
				final Model model) throws IOException, InterruptedException {
			for (Triple triple : model.getGraph().find(Triple.ANY).toList()) {
				if (triple.getSubject().toString().startsWith(LOBID_RESOURCE)) {
					final String objString = triple.getObject().toString();
					final String objResult =
							triple.getObject().isURI() ? wrapped(objString)
									: objString;
					final Text predAndObj =
							new Text(wrapped(triple.getPredicate().toString())
									+ objResult + ".");
					context.write(new Text(wrapped(triple.getSubject()
							.toString())), predAndObj);
				}
			}
		}

		private String wrapped(final String string) {
			return "<" + string + ">";
		}

		private Model resolveUrisToLiterals(final Model model) {
			final FSLNSResolver nsr = new FSLNSResolver();
			nsr.addPrefixBinding("dc", "http://purl.org/dc/elements/1.1/");
			nsr.addPrefixBinding("gnd",
					"http://d-nb.info/standards/elementset/gnd#");
			final FSLHierarchyStore fhs = new FSLHierarchyStore();
			final FSLJenaEvaluator fje = new FSLJenaEvaluator(nsr, fhs);
			fje.setModel(model);
			for (String fslPath : FSL_PATHS) {
				final FSLPath path =
						FSLPath.pathFactory(fslPath, nsr, FSLPath.NODE_STEP);
				String newPredicate =
						"http://purl.org/dc/elements/1.1/creator#"
								+ fslPath.substring(
										fslPath.indexOf("gnd:") + 4,
										fslPath.lastIndexOf('/'));
				addResolvedTriples(model, fje, path, newPredicate);
			}
			return model;
		}

		private Model addResolvedTriples(final Model model,
				final FSLJenaEvaluator fje, final FSLPath path,
				final String newPredicate) {
			@SuppressWarnings("unchecked")
			/* API returns raw Vectors (same reason for NOPMDs below) */
			final Vector<Vector<Object>> pathInstances = fje.evaluatePath(path); // NOPMD
			for (Vector<Object> object : pathInstances) { // NOPMD
				final Triple resolved =
						Triple.create(Node.createURI(object.firstElement()
								.toString()), Node.createURI(newPredicate),
								Node.createLiteral(object.lastElement()
										.toString()));
				model.getGraph().add(resolved);
			}
			return model;
		}
	}
}
