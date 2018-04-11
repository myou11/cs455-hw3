package cs455.hadoop.getInput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class CarrierDelayMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // turn line into a string
        String[] dataRow = value.toString().split("\\s+");

        String[] question_carrier = dataRow[0].split(":");
        String question = question_carrier[0];

        if (question.equals("q4")) {
            String carrier = question_carrier[1];
			String delay_numDelays = dataRow[1];

			/*
			 * Don't figure out avg carrier delay here b/c we need to find the
			 * carriers with the most number of delays and the number of minutes
			 * lost to delays, so the Reducer will need numDelays and delays to do
			 * that. Highest avg delay can be figured out in the setup and cleanup
			 * of the Reducer. Have a TreeMap that stores avg delays for each carrier.
			 * choose the last key (largest) from it to find carrier w/ highest avg delay.
			 * 
			 * double totalDelay = delay / numDelays;
			 */

            context.write(new Text(carrier), new Text(delay_numDelays));
        }
    }
}
