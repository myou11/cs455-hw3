package cs455.hadoop.getInput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class SortBusiestAirportsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // turn line into a string
        // parse the output from the previous job; key-value separated by whitespace; regex to split on whitespace
        String[] dataRow = value.toString().split("\\s+");

		String[] question_airport = dataRow[0].split(":");
		String question = question_airport[0];

		if (question.equals("q3")) {
			String airport = question_airport[1];
			long count = Long.parseLong(dataRow[1]);

			context.write(new LongWritable(count), new Text(airport));
		}
    }
}
