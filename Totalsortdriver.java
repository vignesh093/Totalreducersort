
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;


public class Totalsortdriver {
	public static void main(String args[]) throws Exception
	{
		if(args.length!=2)
			{
			System.err.println("Usage: Worddrivernewapi <input path> <output path>");
			System.exit(-1);
			}
		Job job=new Job();
		
		Configuration conf = job.getConfiguration();
		job.setJarByClass(Totalsortdriver.class);
		job.setJobName("Totalsortdriver");
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.setMapperClass(sortmapper.class);
		job.setReducerClass(sortreducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);
		//Path inputDir = new Path(args[0]);
	       Path partitionFile = new Path("/user/hduser/test","_partitions");
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		InputSampler.Sampler<IntWritable,FloatWritable> sampler= new InputSampler.RandomSampler<IntWritable,FloatWritable>(0.1,1000,10);
		InputSampler.writePartitionFile(job, sampler);
		
		//Add to cache
	
		URI partitionuri=new URI(partitionFile.toString()+"#"+"_partitions");
		DistributedCache.addCacheFile(partitionuri,conf);
		DistributedCache.createSymlink(conf);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
