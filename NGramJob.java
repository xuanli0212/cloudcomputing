
/*Reference:
 *https://github.com/bfemiano/big-data-code-examples/blob/master/Ch4/src/main/java/NGram.java
 * Xuan Li
 * Cloud Computing, Spring 2016
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.StringTokenizer;

public class NGramJob implements Tool {
	private Configuration configuration;
	public static final String NAME = "ngram";
	private static final String GRAM_LENGTH = "number_of_grams";
	public NGramJob(Configuration conf) {
		this.configuration = conf;
	}
	public void setConf(Configuration conf) {
		this.configuration = conf;
	}
	public Configuration getConf() {
		return configuration;
	}
	
	/*
	 * In Mapper function,we tokenize each word and generate n-grams. 
	 * We follow the below assumptions: 
	 * 1. All the words are consecutive alphabetic English words. 
	 * 2. All numbers and special characters ( %@<>'"":., etc) are eliminated. 
	 * 3. Upper case and lower case are not differentiated. 
	 * 4. No words are separated by two lines.
	 */

	public static class charMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static int gramLength;
		private Text ngram = new Text("");
		private IntWritable one = new IntWritable(1);

		// get N-Gram number
		protected void setup(Context context) throws IOException, InterruptedException {
			gramLength = context.getConfiguration().getInt(NGramJob.GRAM_LENGTH, 0);
		}

		@Override
		protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
			String delimiters = " \u00A0\t\r\n\f~`!@#$%^&*()[{]}/?=+\\|-_'\",<.>;:";
			StringTokenizer token = new StringTokenizer(line.toString(), delimiters);

			while (token.hasMoreTokens()) {
				String word = token.nextToken();
				word = word.toLowerCase();

				int wordLength = word.length();

				for (int k = 0; k + gramLength <= wordLength; k++) {
					ngram.set(word.substring(k, k + gramLength));
					context.write(ngram, one);
				}
			}
		}
	}

	// Reducer Function: calculate the frequency
	public static class reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text ngram, Iterable<IntWritable> counts, Context ctx)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts)
				sum += count.get();
			ctx.write(ngram, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("Please follow this: ngram <input> <output> <number_of_grams>");
			System.exit(1);
		}
		ToolRunner.run(new NGramJob(new Configuration()), args);
	}
	

	/*
	 * Main Program. Input jar ngram <input> <output> <number of ngrams>
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
		// set inputpath,outputpath and ngrams number
		String inputPath = args[0];
		String outputPath = args[1];
		configuration.setInt(GRAM_LENGTH, Integer.parseInt(args[2]));
		Job job = new Job(configuration, "NGrams");
		// set input format and path
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));

		job.setJarByClass(NGramJob.class);
		job.setMapperClass(NGramJob.charMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// set output path and format
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		output.setOutputPath(job, new Path(outputPath));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	//
	public static class output extends TextOutputFormat<Text, IntWritable> {

	}

}
