package cs455.hadoop.getInput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GetInputMapper extends Mapper<LongWritable, Text, Text, Text> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataRow = value.toString().split(",");

        if (dataRow[0].equals("Year")) {
            return;
        }

        // defer checking not available values to the reducer
        /*if (dataRow[3].equals("NA") || dataRow[15].equals("NA")) {
            return;
         */
        String month = dataRow[2];

        // time format: hhmm
        String timeStr = dataRow[3];
        String hour = "";
        if (timeStr.length() == 3) {
            // hmm
            hour = timeStr.substring(0, 1);
        } else if (timeStr.length() == 4) {
            // hhmm
            hour = timeStr.substring(0, 2);
        }

        String day = dataRow[3];

        String delay = dataRow[15];
        // TODO: JAVA HEAP SPACE ERROR

        String monthDayTimeDelay = String.format("%s,%s,%s,%s", month, day, hour, delay);

        context.write(new Text("Q1|2"), new Text(monthDayTimeDelay));
    }
}
