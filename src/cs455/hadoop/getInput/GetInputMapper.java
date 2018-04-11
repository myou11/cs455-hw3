package cs455.hadoop.getInput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GetInputMapper extends Mapper<LongWritable, Text, Text, Text> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataRow = value.toString().split(",");

        if (dataRow.length < 29 || dataRow[0].equals("Year")) {
            return;
        }

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

		int year = Integer.parseInt(dataRow[0]);
		int before1998 = (year < 1998) ? 1 : 0;

		// We give Q3 as a normal 1
		Text one = new Text("1");

		// Tack an x onto the end b/c this is how we will differentiate the <question_airport, count_x>
		// from this mapper and the other mapper which generates <question_airport, city>.
		// When one_x gets split, if its length is 2, we know it came from this mapper.
		// Otherwise, the length will be 1 and we will know the value is a city from GetAirportStateMapper.
		Text one_x = new Text("1_x");

		// if the origin is available
		if (!dataRow[16].equals("NA")) {
			String origin = dataRow[16];
			String q3OriginBefore1998= String.format("q3:%s:%d", origin, before1998);
			context.write(new Text(q3OriginBefore1998), one);

			// Q6 - weather delay
			if (!dataRow[25].equals("NA")) {
				int weatherDelay = Integer.parseInt(dataRow[25]);
				// only count the weather delay if it is greater than 0, otherwise it is not a delay
				if (weatherDelay > 0) {
					String q6Origin = "q6:" + origin;
					context.write(new Text(q6Origin), one_x);
				}
            }
		}

		// if the dest is available
		if (!dataRow[17].equals("NA")) {
		    String dest = dataRow[17];
			String q3DestBefore1998 = String.format("q3:%s:%d", dest, before1998);
			context.write(new Text(q3DestBefore1998), one);

			// Q6 - weather delay
			if (!dataRow[25].equals("NA")) {
				// only count the weather delay if it is greater than 0, otherwise it is not a delay
				int weatherDelay = Integer.parseInt(dataRow[25]);
				if (weatherDelay > 0) {
					String q6Dest = "q6:" + dest;
					context.write(new Text(q6Dest), one_x);
				}
            }
		}

		// Q4 - carrier delays
        if (!dataRow[8].equals("NA")) {
		    String question_carrier = "q4:" + dataRow[8];

			// Need to make sure delay is available and that there actually was a delay
		    if (!dataRow[24].equals("NA")) {
				// safe to parse an int after we check that it is available
				int delay = Integer.parseInt(dataRow[24]);

				// Don't want to calculate delays of 0 or less into the avg carrier delays
				if (delay > 0) {
					String delay_numDelays = dataRow[24] + "_1";
					context.write(new Text(question_carrier), new Text(delay_numDelays));
				}
            }
        }

		///*
		// Q5 - Do older planes cause more delays?
		if (!dataRow[10].equals("NA") && !dataRow[10].isEmpty()) {
			String question_tailNum = String.format("q5:%s", dataRow[10]);
			
			// TODO: Use MultipleInputs Hadoop to fetch data from plane-data.csv
			// Designate another input using MultipleInput from the Hadoop Library
			// Have a different mapper perform the mapping of the plane-data

			// Only consider arrival delay for on-time performance
			if (!dataRow[0].equals("NA") && !dataRow[14].equals("NA")) {
				String year_arrDelay_count = String.format("%s_%s_1", dataRow[0], dataRow[14]);
				context.write(new Text(question_tailNum), new Text(year_arrDelay_count));
			}
		}
		//*/

		// Q7 - What are the busiest airports for each month?
		if (!dataRow[1].equals("NA")) {
			String question_month = "q7:" + dataRow[1];

			if (!dataRow[16].equals("NA")) {
				String origin_count = dataRow[16] + "_1";
				context.write(new Text(question_month), new Text(origin_count));
			}

			if (!dataRow[17].equals("NA")) {
				String dest_count = dataRow[17] + "_1";
				context.write(new Text(question_month), new Text(dest_count));
			}
		}
    }
}
