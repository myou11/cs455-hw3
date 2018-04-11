package cs455.hadoop.getInput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives TODO: add documentation here
 */
public class DelayReducer extends Reducer<Text, Text, Text, Text> {

	// Q1, Q2 data structures
	
	// Delay, {month,day,time} maps to find the best/worst delays
	private TreeMap<Double, String> monthDelays;
	private TreeMap<Double, String> dayDelays;
	private TreeMap<Double, String> hourDelays;
	
	// Q4 data structures
	
	// <avgDelay, carrier> map to find the highest avg delays
	private TreeMap<Double, String> avgCarrierDelays;
	
	// <numDelays, carrier> map to find the highest number of delays
	private TreeMap<Long, String> numCarrierDelays;

	// <carrier, totalMinutes> map to get total minutes lost to delay for a carrier
	private TreeMap<String, Long> numMinutesLostToDelays;

	// Q5 - Map to hold delay data for each flight
	// key: tailNum_flightYear, value: arrDelay_depDelay_count
	private HashMap<String, String> flightYearDelayDataMap;

	// Q5 - Map to identify the manufacture year of a tail number
	// key: tailNum, value: manufacture year
	private HashMap<String, String> tailNumManufactureYear;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		monthDelays = new TreeMap<>();
		dayDelays = new TreeMap<>();
		hourDelays = new TreeMap<>();

		avgCarrierDelays = new TreeMap<>();
		numCarrierDelays = new TreeMap<>();
		numMinutesLostToDelays = new TreeMap<>();
	}

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// key_split will be the question, delimited by a colon, and the variables of interest for that question
		// e.g. For question 3, a key could look like "q3:airport" b/c we are interested in busiest airports
	    String[] key_split = key.toString().split(":");
		String question = key_split[0];

		if (question.equals("q1q2")) {
			String monthDayHour = key_split[1];

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

		else if (question.equals("q4")) {
			String carrier = key_split[1];

			for (Text val : values) {
				String[] delay_numDelays = val.toString().split("_");
				double delay = Double.parseDouble(delay_numDelays[0]);
				long numDelays = Long.parseLong(delay_numDelays[1]);

				// TODO: delays could collide, therefore overwriting the delay for another carrier
				// TODO: might not be a big problem because we are only finding the highest
				// TODO: delay, so overwriting means we just found a delay that is equal to
				// TODO: what was there previously, and only carriers are contained in here,
				// TODO: so overwriting another carrier isn't a big problem since the new carrier
				// TODO: technically ties the previous highest delay, if it even overwrote the
				// TODO: previous highest delay

				double avgDelay = delay / numDelays;
				avgCarrierDelays.put(avgDelay, carrier);

				numCarrierDelays.put(numDelays, carrier);
				numMinutesLostToDelays.put(carrier, Long.parseLong(delay_numDelays[0]));
			}
		}

		else if (question.equals("q5")) {
			// indication whether the delay is for old or new planes
			// The reduce function for this question will only be called twice
			// b/c there are only 2 possible values, "q5:old" or "q5:new"
			String oldNew = key_split[1];

			// one of these has to be double so we can do an avg later (don't want to do integer division)
			long cumulativeArrDelay = 0;
			double cumulativeCount = 0;

			for (Text val : values) {
				String[] arrDelay_count = val.toString().split("_");
				long arrDelay = Long.parseLong(arrDelay_count[0]);
				long count = Long.parseLong(arrDelay_count[1]);

				cumulativeArrDelay += arrDelay;
				cumulativeCount += count;
			}
			Double avgDelay = cumulativeArrDelay / cumulativeCount;

			String oldNewMsg = oldNew + " planes average delay";
			context.write(new Text(oldNewMsg), new Text());
			context.write(new Text(String.valueOf(avgDelay)), new Text());
		}
    }

    @Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		// Q1 - Best time to minimize/maximize delay

		// month delay
		if (!monthDelays.isEmpty()) {
			// best month to minimize delays
			context.write(new Text("Best month to minimize delays"), new Text());

			context.write(new Text(monthDelays.firstEntry().getValue()),
					new Text(String.valueOf(monthDelays.firstKey())));

			// best month to maximize delays
			context.write(new Text("\nBest month to maximize delays"), new Text());

			context.write(new Text(monthDelays.lastEntry().getValue()),
					new Text(String.valueOf(monthDelays.lastKey())));
		}

		// day delay
		if (!dayDelays.isEmpty()) {
			// best day to minimize delays
			context.write(new Text("\nBest day to minimize delays"), new Text());

			context.write(new Text(dayDelays.firstEntry().getValue()),
					new Text(String.valueOf(dayDelays.firstKey())));
			// best day to maximize delays
			context.write(new Text("\nBest day to maximize delays"), new Text());

			context.write(new Text(dayDelays.lastEntry().getValue()),
					new Text(String.valueOf(dayDelays.lastKey())));
		}

		// hour delay
		if (!hourDelays.isEmpty()) {
			// best hour to minimize delays
			context.write(new Text("\nBest hour to minimize delays"), new Text());

			context.write(new Text(hourDelays.firstEntry().getValue()),
					new Text(String.valueOf(hourDelays.firstKey())));
			// best hour to maximize delays
			context.write(new Text("\nBest hour to maximize delays"), new Text());

			context.write(new Text(hourDelays.lastEntry().getValue()),
					new Text(String.valueOf(hourDelays.lastKey())));
		}

		// Q4 - carrier delays

		// Find carriers with most delays (total # of delayed flights, total # of minutes lost to delays)

		TreeMap<Long, String> reversedNumCarrierDelays = new TreeMap(Collections.reverseOrder());
		reversedNumCarrierDelays.putAll(numCarrierDelays);

		context.write(new Text("\nCarriers with the most delays (carrier, numDelays_totalMinutesLostToDelays)"), new Text());

		int carrierCounter = 0;
		for (Map.Entry<Long, String> entry : reversedNumCarrierDelays.entrySet()) {
			if (carrierCounter == 10)
				break;
			
			String carrier = entry.getValue();
			long numDelays = entry.getKey();
			long minutesLostToDelay = numMinutesLostToDelays.get(carrier);

			String numDelays_minutesLostToDelay = String.format("%d_%d", numDelays, minutesLostToDelay);

			context.write(new Text(carrier), new Text(numDelays_minutesLostToDelay));

			carrierCounter++;
		}

		// Find carrier with highest avg delay

		context.write(new Text("\nCarrier with the highest average delay"), new Text());

		Map.Entry<Double, String> highestAvgCarrierDelay = avgCarrierDelays.lastEntry();
		String carrier = highestAvgCarrierDelay.getValue();
		double highestAvgDelay = highestAvgCarrierDelay.getKey();

		context.write(new Text(carrier), new Text(String.valueOf(highestAvgDelay)));
	}
}
