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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucsc.hadoop.util.ConfigurationUtil;


/**
 * A MapReduce application to list how many actors appeared in a given movie
 * 
 * @author john
 *
 */
public class ActorList extends Configured implements Tool {
	
	private static final Log LOG = LogFactory.getLog(ActorList.class);
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 2) {
			System.err.println("Usage: actorlist <in> <out>");
			System.exit(2);
		}
		
		// delete output folder
		File outputFolder = new File(args[1]);
		FileUtils.deleteDirectory(outputFolder);
		outputFolder = null;
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.info("input: " + args[0] + " output: " + args[1]);
		
		Job job = new Job(conf, "movie count");
		job.setJarByClass(ActorList.class);
		job.setMapperClass(MovieTokenizerMapper.class);
		job.setReducerClass(MovieYearReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		
		LOG.info("****************    Finished ActorList MapReduce");

		return (result) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ActorList(), args);
		System.exit(exitCode);
	}
	
	public static class MovieTokenizerMapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			
			if (tokens.length == 3) {
				String enclosedName = "(" + tokens[0] + ")";
 				context.write(new Text(tokens[1]), new Text(enclosedName));
			}
		}
	}
	
	public static class MovieYearReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text movie, Iterable<Text> actors, Context context) 
				 throws IOException, InterruptedException {
				
			StringBuilder sb = new StringBuilder(1000);
			for (Text actor : actors) {
				sb.append(actor);
				sb.append(",");
			}
			context.write(movie, new Text(sb.toString()));
			sb.setLength(0); 
		}
	}
}
