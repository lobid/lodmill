/* Copyright 2012-2014 Fabian Steeg, hbz. Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill.hadoop;

import static java.lang.String.format;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.lobid.lodmill.JsonLdConverter;
import org.lobid.lodmill.JsonLdConverter.Format;
import org.lobid.lodmill.hadoop.CollectSubjects.CollectSubjectsMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.io.CharStreams;
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
	private String indexName;
	private Client client = CLIENT;
	private String aliasSuffix = "-testing";
	private boolean update = false;
	private static final String ES_NODE = "193.30.112.171";

	// TODO pass params
	private static final InetSocketTransportAddress NODE_1 =
			new InetSocketTransportAddress(ES_NODE, 9300);
	private static final InetSocketTransportAddress NODE_2 =
			new InetSocketTransportAddress("193.30.112.172", 9300);
	private static final TransportClient TC = new TransportClient(
			ImmutableSettings.settingsBuilder().put("cluster.name", "quaoar")
					.put("client.transport.sniff", false)
					.put("client.transport.ping_timeout", 20, TimeUnit.SECONDS).build());
	private static final Client CLIENT = TC.addTransportAddress(NODE_1)
			.addTransportAddress(NODE_2);
	/** JSON key to use internally to identify the ES parent during indexing. */
	public static final String INTERNAL_PARENT = "internal_parent";
	/** JSON key to use internally to identify the ES document during indexing. */
	public static final String INTERNAL_ID = "internal_id";

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
		if (args.length != 5) {
			System.err
					.println("Usage: NTriplesToJsonLd"
							+ " <input path> <index name> <index type> <target subjects prefix> <index alias suffix>");
			System.exit(-1);
		}
		conf = getConf();
		indexName = args[1];
		String indexType = args[2];
		aliasSuffix = args[4];
		update = args[0].toLowerCase().contains("update");
		if (update) {
			getNewestIndex();
		} else
			createIndex();
		conf.setStrings("mapred.textoutputformat.separator", NEWLINE);
		conf.setStrings("target.subject.prefix", args[3]);
		conf.set(INDEX_TYPE, indexType);
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		conf.set("es.nodes", ES_NODE + ":9200");
		conf.set(INDEX_NAME, indexName);
		conf.set("es.resource", indexName + "/" + indexType);
		conf.set("es.input.json", "yes");
		conf.set("es.mapping.id", INTERNAL_ID);

		if (indexType.equals("json-ld-lobid-item"))
			conf.set("es.mapping.parent", INTERNAL_PARENT);
		final String mapFileName = CollectSubjects.mapFileName(indexName);
		conf.setStrings("map.file.name", mapFileName);
		final Job job = Job.getInstance(conf);
		final Path mapFilePath = new Path(mapFileName);
		if (FileSystem.get(conf).exists(mapFilePath)) {
			job.addCacheFile(mapFilePath.toUri());
		}
		job.setNumReduceTasks(NODES * SLOTS);
		job.setJarByClass(NTriplesToJsonLd.class);
		job.setJobName("LobidToJsonLd");
		FileInputFormat.addInputPaths(job, args[0]);
		job.setOutputFormatClass(EsOutputFormat.class);
		job.setMapperClass(NTriplesToJsonLdMapper.class);
		job.setReducerClass(NTriplesToJsonLdReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		setIndexRefreshInterval(CLIENT, "-1");
		LOG.info(String.format("Process: index %s, type %s", indexName, indexType));
		boolean success = job.waitForCompletion(true);
		if (success) {
			if (!aliasSuffix.equals("NOALIAS") || !update
					|| !aliasSuffix.toLowerCase().contains("test"))
				updateAliases(indexName, aliasSuffix);
			client.admin().indices().prepareRefresh(indexName).execute().actionGet();
			setIndexRefreshInterval(CLIENT, "1000");
		}
		System.exit(success ? 0 : 1);
		return 0;
	}

	private void getNewestIndex() {
		String indexNameWithoutTimestamp = indexName.replaceAll("20.*", "");
		final SortedSetMultimap<String, String> indices = groupByIndexCollection();
		for (String prefix : indices.keySet()) {
			final SortedSet<String> indicesForPrefix = indices.get(prefix);
			final String newestIndex = indicesForPrefix.last();
			if (newestIndex.startsWith(indexNameWithoutTimestamp))
				indexName = newestIndex;
		}
		LOG.info("Going to UPDATE existing index " + indexName);
	}

	private void createIndex() {
		IndicesAdminClient adminClient = CLIENT.admin().indices();
		if (!adminClient.prepareExists(indexName).execute().actionGet().isExists()) {
			LOG.info("Going to CREATE new index " + indexName);
			adminClient.prepareCreate(indexName).setSource(config()).execute()
					.actionGet();
		}
	}

	private static String config() {
		String res = null;
		try {
			final InputStream config =
					Thread.currentThread().getContextClassLoader()
							.getResourceAsStream("index-config.json");
			try (InputStreamReader reader = new InputStreamReader(config, "UTF-8")) {
				res = CharStreams.toString(reader);
			}
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return res;
	}

	private void setIndexRefreshInterval(Client client, String setting) {
		client
				.admin()
				.indices()
				.prepareUpdateSettings(indexName)
				.setSettings(
						ImmutableSettings.settingsBuilder().put("index.refresh_interval",
								setting)).execute().actionGet();

	}

	private void updateAliases(final String name, final String suffix) {
		final SortedSetMultimap<String, String> indices = groupByIndexCollection();
		for (String prefix : indices.keySet()) {
			final SortedSet<String> indicesForPrefix = indices.get(prefix);
			final String newIndex = indicesForPrefix.last();
			final String newAlias = prefix + suffix;
			LOG.info(format("Prefix '%s', newest index: %s", prefix, newIndex));
			removeOldAliases(indicesForPrefix, newAlias);
			if (!newIndex.equals(indicesForPrefix))
				createNewAlias(newIndex, newAlias);
			deleteOldIndices(name, indicesForPrefix);
		}
	}

	private SortedSetMultimap<String, String> groupByIndexCollection() {
		final SortedSetMultimap<String, String> indices = TreeMultimap.create();
		for (String index : client.admin().indices().prepareStats().execute()
				.actionGet().getIndices().keySet()) {
			final String[] nameAndTimestamp = index.split("-(?=\\d)");
			indices.put(nameAndTimestamp[0], index);
		}
		return indices;
	}

	private void removeOldAliases(final SortedSet<String> indicesForPrefix,
			final String newAlias) {
		for (String name : indicesForPrefix) {
			final Set<String> aliases = aliases(name);
			for (String alias : aliases) {
				if (alias.equals(newAlias)) {
					LOG.info(format("Delete alias index,alias: %s,%s", name, alias));
					client.admin().indices().prepareAliases().removeAlias(name, alias)
							.execute().actionGet();
				}
			}
		}
	}

	private void createNewAlias(final String newIndex, final String newAlias) {
		LOG.info(format("Create alias index,alias: %s,%s", newIndex, newAlias));
		client.admin().indices().prepareAliases().addAlias(newIndex, newAlias)
				.execute().actionGet();
	}

	private void deleteOldIndices(final String name,
			final SortedSet<String> allIndices) {
		if (allIndices.size() >= 3) {
			final List<String> list = new ArrayList<>(allIndices);
			list.remove(name);
			for (String indexToDelete : list.subList(0, list.size() - 2)) {
				if (aliases(indexToDelete).isEmpty()) {
					LOG.info(format("Deleting index: " + indexToDelete));
					client.admin().indices()
							.delete(new DeleteIndexRequest(indexToDelete)).actionGet();
				}
			}
		}
	}

	private Set<String> aliases(final String name) {
		final ClusterStateRequest clusterStateRequest =
				Requests.clusterStateRequest().nodes(true).indices(name);
		return Sets.newHashSet(client.admin().cluster().state(clusterStateRequest)
				.actionGet().getState().getMetaData().aliases().keysIt());
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
			final URI mapFile = findMapFile(context.getCacheFiles(), rawMapFile);
			if (mapFile != null)
				initMapFileReader(new Path(mapFile));
			else
				LOG.warn("No subjects cache files found!");
		}

		private static URI findMapFile(final URI[] localCacheFiles,
				final String rawMapFileName) {
			if (localCacheFiles == null || rawMapFileName == null)
				return null;
			for (URI uri : localCacheFiles)
				if (uri.toString().contains(rawMapFileName))
					return uri;
			return null;
		}

		private void initMapFileReader(final Path mapFilePath) throws IOException,
				FileNotFoundException {
			LOG.info("Reading map file from: " + mapFilePath);
			reader = new MapFile.Reader(mapFilePath, CollectSubjects.MAP_FILE_CONFIG);
			if (reader == null)
				throw new IllegalStateException(String.format(
						"Could not load map file data from %s", mapFilePath));
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
			String subjectString = triple.getSubject().toString();
			return triple.getSubject().isURI()
					&& (subjectString.startsWith(prefix == null ? "" : prefix) //
					&& !subjectString.endsWith("/about"));
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
			final String id =
					key.toString().substring(1, key.toString().length() - 1);
			final String parentProperty =
					context.getConfiguration().get("es.mapping.parent") == null ? null
							: CollectSubjects.PARENTS.iterator().next();
			final String jsonLd =
					new JsonLdConverter(Format.N_TRIPLE).toJsonLd(triples,
							parentProperty, id);
			context.write(new Text(""), new Text(jsonLd));
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
