package cs455.hadoop.getInput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class DelayMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // turn line into a string
        String[] dataRow = value.toString().split("\\s+");

		// key
		String[] question_category = dataRow[0].split(":");
		
		String question = question_category[0];

		if (question.equals("q1q2")) {
			String[] delay_numDelays = dataRow[1].split("_");
			double delay = Double.parseDouble(delay_numDelays[0]);
			long numDelays = Long.parseLong(delay_numDelays[1]);

			double avgDelay = delay / numDelays;

			context.write(new Text(dataRow[0]), new Text(String.valueOf(avgDelay)));
		}

		if (question.equals("q4")) {
			// value
			String delay_numDelays = dataRow[1];

			context.write(new Text(dataRow[0]), new Text(delay_numDelays));
		}

		// TODO: Might want to do this in a separate job so we can utilize multiple reducers. This job pipes output to one reducer only
		if (question.equals("q5")) {
			String arrDelay_count= dataRow[1];

			context.write(new Text(dataRow[0]), new Text(arrDelay_count));
		}
    }
}
