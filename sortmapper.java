import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class sortmapper extends Mapper<LongWritable,Text,IntWritable,FloatWritable> 
{
public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
{
	String s=value.toString();
	String[] s1=s.split(",");
	context.write(new IntWritable(Integer.parseInt(s1[0])), new FloatWritable(Float.parseFloat(s1[1])));
}
}
