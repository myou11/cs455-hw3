package cs455.hadoop.busiestAirports;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives TODO: add documentation here
 */
public class TopTenBusiestAirportsReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

	private int counter;

	@Override
	protected void setup(Context context) {
		counter = 0;
	}

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		if (counter == 10) {
			return;
		}

		for (Text val : values) {
			context.write(val, key);
			counter++;
			if (counter == 10) {
				break;
			}
		}
    }
}
