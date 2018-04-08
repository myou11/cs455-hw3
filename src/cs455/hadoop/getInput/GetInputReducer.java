package cs455.hadoop.getInput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GetInputReducer extends Reducer<Text, Text, Text, Text> {
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		System.out.println(key.toString());
		String[] keySplit = key.toString().split(":");

		String question = keySplit[0];

		if (question.equals("q1q2") || question.equals("q4")) {
			long cumulativeDelay = 0;
			long cumulativeNumDelays = 0;

			for (Text val : values) {
				String[] delay_numDelays = val.toString().split("_");
				long delay = Long.parseLong(delay_numDelays[0]);
				long numDelays = Long.parseLong(delay_numDelays[1]);

				cumulativeDelay += delay;
				cumulativeNumDelays += numDelays;
			}

			// NOTE: CAN'T HAVE DO DIVISION IN THIS CLASS B/C IT IS USED AS A COMBINER
			// COMBINERS MUST BE COMMUTATIVE AND ASSOCIATIVE (DIVISION IS NOT COMMUTATIVE)
			//cumulativeDelay /= numDelays;

			// Instead of dividing cumulativeDelay here, wait until the cleanup function
			// just keep track of total number of delays
			String delay_numDelays = String.format("%d_%d", cumulativeDelay, cumulativeNumDelays);

			context.write(key, new Text(delay_numDelays));
		}

		if (question.equals("q3") || question.equals("q6")) {
			long count = 0;
			for (Text val : values) {
				count += Long.parseLong(val.toString());
			}
			context.write(key, new Text(String.valueOf(count)));
		}
	}

	/*
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
    	while (context.nextKeyValue()) {
    		String[] keySplit = context.getCurrentKey().toString().split("");
    		String question = keySplit[0];
    		String questionCategory = keySplit[1];

    		if (question.equals("q1q2")) {
    			String[] delay_numDelays = context.getCurrentValue().toString().split("_");
    			double delay = Double.parseDouble(delay_numDelays[0]);
    			long numDelays = Long.parseLong(delay_numDelays[1]);

				// avg delay
    			double totalDelay = delay / numDelays;

    			context.write(new Text(questionCategory), new Text(String.valueOf(totalDelay)));
			}
		}
	}
	*/
}
