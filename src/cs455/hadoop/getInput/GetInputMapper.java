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

		System.out.println("CAN I PLEASE GET PRINTED??");

		// Q1 / Q2 - best/worst month/day/hour to minimize/maximize delays
        // only consider parsing the month, day, or time if the delay is available
        if (!dataRow[15].equals("NA")) {
            // delay: "delay_1"
            String delay_numDelays = dataRow[15] + "_1";

            // month delay
            if (!dataRow[1].equals("NA")) {
                // append 'M_' to indicate that these are months
                String month = "q1q2:M_" + dataRow[1];
                context.write(new Text(month), new Text(delay_numDelays));
            }

            // day delay
            if (!dataRow[3].equals("NA")) {
                // append 'D_' to indicate that these are days
                String day = "q1q2:D_" + dataRow[3];
                context.write(new Text(day), new Text(delay_numDelays));
            }

            // hour delay
            if (!dataRow[4].equals("NA")) {
				// pad times to length 4 (hhmm)
                String time = String.format("%04d", Integer.parseInt(dataRow[4]));

                // append 'H_' to indicate that these are hours
                String hour = "q1q2:H_";

				// ensure the hours are within the range 0-23
				hour += Integer.parseInt(time.substring(0, 2)) % 24;

                context.write(new Text(hour), new Text(delay_numDelays));
            }
        }

		// Q3 - busiest airports
		String origin = dataRow[16];
		String dest = dataRow[17];
		Text one = new Text("1");

		// if the origin is available
		if (!origin.equals("NA")) {
			String q3Origin = "q3:" + origin;
			context.write(new Text(q3Origin), one);

			// Q6 - weather delay
			if (!dataRow[25].equals("NA") /*|| !dataRow[26].equals("NA")*/) {
			    String q6Origin = "q6:" + origin;
			    context.write(new Text(q6Origin), one);
            }
		}

		// if the dest is available
		if (!dest.equals("NA")) {
			String q3Dest = "q3:" + dest;
			context.write(new Text(q3Dest), one);

			// Q6 - weather delay
			if (!dataRow[25].equals("NA") /*|| !dataRow[26].equals("NA")*/) {
			    String q6Dest = "q6:" + dest;
			    context.write(new Text(q6Dest), one);
            }
		}

		// Q4 - carrier delays
        if (!dataRow[8].equals("NA")) {
		    String carrier = "q4:" + dataRow[8];

		    if (!dataRow[24].equals("NA")) {
		        String delay_numDelays = dataRow[24] + "_1";
		        context.write(new Text(carrier), new Text(delay_numDelays));
            }
        }
    }
}
