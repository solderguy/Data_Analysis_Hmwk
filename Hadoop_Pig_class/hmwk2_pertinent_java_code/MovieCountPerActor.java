package ucsc.hadoop.homework2;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucsc.hadoop.util.ConfigurationUtil;


/**
 * A MapReduce application to count how many movies an actor has appeared in
 * 
 * Accomplishes the sort order by doing a second job with a second mapper (no reduce)
 * @author john
 *
 */
public class MovieCountPerActor extends Configured implements Tool {
	
	private static final Log LOG = LogFactory.getLog(MovieCountPerActor.class);
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 2) {
			System.err.println("Usage: moviecountperactor <in> <out>");
			System.exit(2);
		}
		// delete temp output folder
		String tempOutputString = args[1] + "/temp";
		File tempOutputFolder = new File(tempOutputString);
		FileUtils.deleteDirectory(tempOutputFolder);
		tempOutputFolder = null;
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.info("input: " + args[0] + " output: " + tempOutputString);
		
		Job job = new Job(conf, "movie count");
		job.setJarByClass(MovieCountPerActor.class);
		job.setMapperClass(MovieTokenizerMapper.class);
		job.setReducerClass(MovieYearReducer.class);

		job.setMapOutputKeyClass(Text.class);								
		job.setMapOutputValueClass(IntWritable.class);						
		
		job.setOutputKeyClass(Text.class);								
		job.setOutputValueClass(IntWritable.class);	
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(tempOutputString));
		
		boolean result = job.waitForCompletion(true);
		LOG.info("****************    Finished first job");
		if (result == false) return 1;
		else {						// only run second job if first succeeds
			String[] run2Args = new String[2];
			run2Args[0] = tempOutputString;
			run2Args[1] = args[1]+"/final";
			int resultJob2 = runJob2(run2Args);
			return resultJob2;
		}
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MovieCountPerActor(), args);
		System.exit(exitCode);
	}

	public static class MovieTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
		
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			
			if (tokens.length == 3) {
				context.write(new Text(tokens[0]), ONE);
			}
		}
	}
	
	public static class MovieYearReducer extends Reducer<Text, IntWritable, IntWritable, Text > {
		private IntWritable result = new IntWritable();
		
		@Override
		public void reduce(Text year, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
				
			int actorCount = 0;
			for (IntWritable count : values) {
				actorCount += count.get();
			}
			result.set(actorCount);
			context.write(result, year);	
		}
	}

	public int runJob2(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 2) {
			System.err.println("Usage: moviecount <in> <out>");
			System.exit(2);
		}
		// delete final output folder
		File finalOutputFolder = new File(args[1]);
		FileUtils.deleteDirectory(finalOutputFolder);
		finalOutputFolder = null;
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.info("input: " + args[0] + " output: " + args[1]);
		
		Job job = new Job(conf, "movie count");
		job.setJarByClass(MovieCountPerActor.class);
		job.setMapperClass(MovieTokenizerMapper2.class);
		//job.setReducerClass(MovieYearReducer.class);  // Don't need reducing, just reordering

		job.setMapOutputKeyClass(IntWritable.class);							
		job.setMapOutputValueClass(Text.class);						
		
		job.setOutputKeyClass(IntWritable.class);									
		job.setOutputValueClass(Text.class);							
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		LOG.info("****************    Finished MovieCountPerActor MapReduce");
		
		return (result) ? 0 : 1;
	}
	
	public static class MovieTokenizerMapper2 extends Mapper<Object, Text, IntWritable, Text> {
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			IntWritable APPEARANCES = new IntWritable(Integer.parseInt(tokens[0]));
			context.write(APPEARANCES, new Text(tokens[1]));							// here
			}
	}
}
