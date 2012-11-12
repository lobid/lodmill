package hadoop.sample.lobid;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LobidToJsonLdMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	@Override
	public final void map(final LongWritable key, final Text value,
			final Context context) throws IOException, InterruptedException {

		final String val = value.toString();
		// we take all non-blank nodes for now, and map them to their triples:
		if (val.startsWith("<http")) {
			final String subject = val.substring(0, val.indexOf('>') + 1);
			context.write(new Text(subject), value);
		}

		// we could get blank nodes like this, but doesn't work with our logic:
		/*
		 * final Model model = ModelFactory.createDefaultModel(); model.read(new
		 * StringReader(value.toString()), null, Format.N_TRIPLE.getName());
		 * final Triple any = Triple.createMatch(null, null, null); final Triple
		 * triple = model.getGraph().find(any).toList().get(0);
		 * context.write(new Text(triple.getSubject().toString(false)), value);
		 */
	}
}