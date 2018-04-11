package cs455.hadoop.getInput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class TopTenAirportsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // turn line into a string
        // parse the output from the previous job; key-value separated by whitespace; regex to split on whitespace
        String[] dataRow = value.toString().split("\\s+");

		// key
		String question_category = dataRow[0];
		// value
		String count = dataRow[1];

		context.write(new Text(question_category), new Text(count));
    }
}
