package hadoop.sample.lobid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LobidToJsonLd implements Tool {

	private static final int NODES = 4; // e.g. 4 nodes in cluster
	private static final int SLOTS = 2; // e.g. 2 cores per node
	private Configuration conf;

	public static void main(final String[] args) throws Exception { // NOPMD
		final int res = ToolRunner.run(new LobidToJsonLd(), args);
		System.exit(res);
	}

	@Override
	public final int run(final String[] args) throws Exception { // NOPMD
		if (args.length != 2) {
			System.err
					.println("Usage: LobidToJsonLd <input path> <output path>");
			System.exit(-1);
		}
		final Configuration conf = getConf();
		conf.setStrings("mapred.textoutputformat.separator", "\n");
		conf.setInt("mapred.tasktracker.reduce.tasks.maximum", SLOTS);
		final Job job = new Job(conf);
		job.setNumReduceTasks(NODES * SLOTS);
		job.setJarByClass(LobidToJsonLd.class);
		job.setJobName("LobidToJsonLd");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(LobidToJsonLdMapper.class);
		job.setReducerClass(LobidToJsonLdReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	@Override
	public final void setConf(final Configuration conf) {
		this.conf = conf;
	}

	@Override
	public final Configuration getConf() {
		return conf;
	}
}