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

        String[] question_time = dataRow[0].split(":");
        String question = question_time[0];

        if (question.equals("q1q2")) {
            String time = question_time[1];

            String[] delay_numDelays = dataRow[1].split("_");
            double delay = Double.parseDouble(delay_numDelays[0]);
            long numDelays = Long.parseLong(delay_numDelays[1]);

            double avgDelay = delay / numDelays;

            context.write(new Text(time), new Text(String.valueOf(avgDelay)));
        }
    }
}
