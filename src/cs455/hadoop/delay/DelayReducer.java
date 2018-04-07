package cs455.hadoop.delay;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.TreeMap;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives TODO: add documentation here
 */
public class DelayReducer extends Reducer<Text, Text, Text, Text> {

	// Delay, {month,day,time} maps to find the best/worst delays
	TreeMap<Double, String> monthDelays;
	TreeMap<Double, String> dayDelays; 
	TreeMap<Double, String> hourDelays;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		monthDelays = new TreeMap<>();
		dayDelays = new TreeMap<>();
		hourDelays = new TreeMap<>();
	}

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalDelay = 0.0;

        int numDelays = 0;
        for (Text val : values) {
            totalDelay += Double.parseDouble(val.toString());
            numDelays++;
        }

        // avg delay
        totalDelay /= numDelays;

		// insert the month, day, hour into their respective maps
		String monthDayHour = key.toString();
		if (monthDayHour.charAt(0) == 'M') {
			monthDelays.put(totalDelay, monthDayHour);
		} else if (monthDayHour.charAt(0) == 'D') {
			dayDelays.put(totalDelay, monthDayHour);
		} else {
			hourDelays.put(totalDelay, monthDayHour);
		}

        System.out.println("inside reduce");
        //context.write(key, new Text(String.valueOf(totalDelay)));
    }

    @Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		/*
		TreeMap<Double, String> monthDelays = new TreeMap<>();
		TreeMap<Double, String> dayDelays = new TreeMap<>();
		TreeMap<Double, String> hourDelays = new TreeMap<>();

		System.out.println("before while (context.nextKeyValue())");
		// group the delays by month, day, hour
        while (context.nextKeyValue()) {
			String monthDayHour = context.getCurrentKey().toString();
			System.out.printf("monthDayHour: %s", monthDayHour);

			if (monthDayHour.charAt(0) == 'M') {
				monthDelays.put(Double.parseDouble(context.getCurrentValue().toString()), monthDayHour);
			} else if (monthDayHour.charAt(0) == 'D') {
				dayDelays.put(Double.parseDouble(context.getCurrentValue().toString()), monthDayHour);
			} else {
				hourDelays.put(Double.parseDouble(context.getCurrentValue().toString()), monthDayHour);
			}
        }
		*/
		System.out.println("after while (context.nextKeyValue())");

		// TODO: LINE 63 RETURNS NULL FOR SOME REASON???
		// month delay
		if (!monthDelays.isEmpty()) {
			context.write(new Text(monthDelays.firstEntry().getValue()), 
					new Text(String.valueOf(monthDelays.firstKey())));
			context.write(new Text(monthDelays.lastEntry().getValue()), 
					new Text(String.valueOf(monthDelays.lastKey())));
		}

		// day delay
		if (!dayDelays.isEmpty()) {
			context.write(new Text(dayDelays.firstEntry().getValue()), 
					new Text(String.valueOf(dayDelays.firstKey())));
			context.write(new Text(dayDelays.lastEntry().getValue()), 
					new Text(String.valueOf(dayDelays.lastKey())));
		}

		// hour delay
		if (!hourDelays.isEmpty()) {
			context.write(new Text(hourDelays.firstEntry().getValue()), 
					new Text(String.valueOf(hourDelays.firstKey())));
			context.write(new Text(hourDelays.lastEntry().getValue()), 
					new Text(String.valueOf(hourDelays.lastKey())));
		}
    }
}
