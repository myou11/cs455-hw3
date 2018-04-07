package cs455.hadoop.delay;

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
        String[] dataRow = value.toString().split(",");

        if (dataRow[0].equals("Year")) {
            return;
        }

        // only consider parsing the month, day, or time if the delay is available
        if (!dataRow[15].equals("NA")) {
            String delay = dataRow[15];

            // month delay
            if (!dataRow[1].equals("NA")) {
                // append 'M_' to indicate that these are months
                String month = "M_" + dataRow[1];
                context.write(new Text(month), new Text(delay));
            }

            // day delay
            if (!dataRow[3].equals("NA")) {
                // append 'D_' to indicate that these are days
                String day = "D_" + dataRow[3];
                context.write(new Text(day), new Text(delay));
            }

            // hour delay
            if (!dataRow[4].equals("NA")) {
				// pad times to length 4 (hhmm)
                String time = String.format("%04d", Integer.parseInt(dataRow[4]));

                // append 'H_' to indicate that these are hours
                String hour = "H_";

				// ensure the hours are within the range 0-23
				hour += Integer.parseInt(time.substring(0, 2)) % 24;

                context.write(new Text(hour), new Text(delay));
            }
        }

        /*
        // dont produce intermediate outputs for values that are not available
        // TODO: will probably not want to return immediately later when we factor in time of day and month too
        // TODO: since an NA in one of these columns does not dictate an NA in the others, so dont want to return before checking the others
        if (dataRow[3].equals("NA") || dataRow[15].equals("NA")) {
            return;
        }

        // day of week
        int day = Integer.parseInt(dataRow[3]);

        // delay for that day
        double delay = Double.parseDouble(dataRow[15]);

        context.write(new IntWritable(day), new Text(String.valueOf(delay)));
        */
    }
}
