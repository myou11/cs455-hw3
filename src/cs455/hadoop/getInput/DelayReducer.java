package cs455.hadoop.getInput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives TODO: add documentation here
 */
public class DelayReducer extends Reducer<Text, Text, Text, Text> {

	// Delay, {month,day,time} maps to find the best/worst delays
	private TreeMap<Double, String> monthDelays;
	private TreeMap<Double, String> dayDelays;
	private TreeMap<Double, String> hourDelays;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		monthDelays = new TreeMap<>();
		dayDelays = new TreeMap<>();
		hourDelays = new TreeMap<>();
	}

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    String monthDayHour = key.toString();

		for (Text val : values) {
			Double delay = Double.parseDouble(val.toString());

	    	// TODO: delays could collide, therefore overwriting the delay for another month
			// TODO: might not be a big problem because we are only finding the best/worst
			// TODO: delays, so overwriting means we just found a delay that is equal to
			// TODO: what was there previously, and only months are contained in here,
			// TODO: so overwriting another month isn't a big problem since the new month
			// TODO: will just be the best/worst delay if it even is
			if (monthDayHour.charAt(0) == 'M')
	    		monthDelays.put(delay, monthDayHour);
			else if (monthDayHour.charAt(0) == 'D')
				dayDelays.put(delay, monthDayHour);
			else {
				hourDelays.put(delay, monthDayHour);
			}
		}
    }


    @Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("after while (context.nextKeyValue())");

		// TODO: LINE 63 RETURNS NULL FOR SOME REASON???
		// month delay
		if (!monthDelays.isEmpty()) {
			// best month to minimize delays
			context.write(new Text(monthDelays.firstEntry().getValue()),
					new Text(String.valueOf(monthDelays.firstKey())));

			// best month to maximize delays
			context.write(new Text(monthDelays.lastEntry().getValue()),
					new Text(String.valueOf(monthDelays.lastKey())));
		}

		// day delay
		if (!dayDelays.isEmpty()) {
			// best month to minimize delays
			context.write(new Text(dayDelays.firstEntry().getValue()),
					new Text(String.valueOf(dayDelays.firstKey())));
			// best day to maximize delays
			context.write(new Text(dayDelays.lastEntry().getValue()),
					new Text(String.valueOf(dayDelays.lastKey())));
		}

		// hour delay
		if (!hourDelays.isEmpty()) {
			// best hour to minimize delays
			context.write(new Text(hourDelays.firstEntry().getValue()),
					new Text(String.valueOf(hourDelays.firstKey())));
			// best hour to maximize delays
			context.write(new Text(hourDelays.lastEntry().getValue()),
					new Text(String.valueOf(hourDelays.lastKey())));
		}
	}
}
