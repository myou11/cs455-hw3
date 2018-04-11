package cs455.hadoop.busiestAirports;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class BusiestAirportsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // turn line into a string
        String[] dataRow = value.toString().split(",");

        if (dataRow[0].equals("Year")) {
            return;
        }

		String origin = dataRow[16];
		String dest = dataRow[17];
		LongWritable one = new LongWritable(1);

		if (!origin.equals("NA")) {
			context.write(new Text(origin), one);
		}

		if (!dest.equals("NA")) {
			context.write(new Text(dest), one);
		}
    }
}
