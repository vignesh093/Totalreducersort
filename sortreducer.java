import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class sortreducer extends Reducer<IntWritable,FloatWritable,IntWritable,FloatWritable>{
public void reduce(IntWritable key,Iterable<FloatWritable> value,Context context) throws IOException,InterruptedException
{
	float f=0;
	for(FloatWritable val:value)
	{
		f+=val.get();
	}
	context.write(key,new FloatWritable(f));
}
}
