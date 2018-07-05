package Test;

import java.io.IOException;

import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class Driver {

			// TODO Auto-generated method stub
		public static void main(String[] args) throws Exception {
			
			Configuration conf = new Configuration();
			String[] newArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (newArgs.length != 4) {
				System.err.println("Arguments not passed correctly!");
				System.exit(2);
			}
			
			conf.set("country1", (newArgs[2]));
			conf.set("country2", (newArgs[3]));
			@SuppressWarnings("deprecation")
			Job job = new Job(conf, "Trending videos Analysis");
			job.setNumReduceTasks(1);
			job.setJarByClass(Driver.class);
			
			job.setMapperClass(Mapper1.class);
			job.setReducerClass(Reducer1.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			
			 
			TextInputFormat.addInputPath(job, new Path(newArgs[0]));
			TextOutputFormat.setOutputPath(job, new Path(newArgs[1]));
			
			//TextInputFormat.addInputPath(job, new Path(args[0]));
			//TextOutputFormat.setOutputPath(job, new Path(args[1]));
			
			 System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}


