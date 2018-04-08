package cs455.hadoop.busiestAirports;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.TreeMap;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives TODO: add documentation here
 */
public class BusiestAirportsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {


	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	}

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long count = 0;

		for (LongWritable val : values) {
			count += val.get();
		}

		context.write(key, new LongWritable(count));
    }

	/*
    @Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
    }
	*/
}
