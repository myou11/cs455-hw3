package cs455.hadoop.getInput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reducer: Input to the reducer is the output from the mapper.
 * Input is <carrier, totalDelay_numDelays> key-value pairs
 * Singleton instance of this class so we can find global highest delays among all carriers
 */
public class CarrierDelayReducer extends Reducer<Text, Text, Text, Text> {

	// <avgDelay, carrier> map to find the highest avg delays
	private TreeMap<Double, String> avgCarrierDelays;
	
	// <numDelays, carrier> map to find the highest number of delays
	private TreeMap<Long, String> numCarrierDelays;

	// <carrier, totalMinutes> map to get total minutes lost to delay for a carrier
	private TreeMap<String, Long> numMinutesLostToDelays;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		avgCarrierDelays = new TreeMap<>();
		numCarrierDelays = new TreeMap<>();
		numMinutesLostToDelays = new TreeMap<>();
	}

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    String carrier = key.toString();

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

    @Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// Find carriers with most delays (total # of delayed flights, total # of minutes lost to delays)
		TreeMap<Long, String> reversedNumCarrierDelays = new TreeMap(Collections.reverseOrder());
		reversedNumCarrierDelays.putAll(numCarrierDelays);

		int counter = 0;
		for (Map.Entry<Long, String> entry : reversedNumCarrierDelays.entrySet()) {
			if (counter == 10)
				break;
			
			String carrier = entry.getValue();
			long numDelays = entry.getKey();
			long minutesLostToDelay = numMinutesLostToDelays.get(carrier);

			String numDelays_minutesLostToDelay = String.format("%d_%d", numDelays, minutesLostToDelay);

			context.write(new Text(carrier), new Text(numDelays_minutesLostToDelay));

			counter++;
		}

		// Find carrier with highest avg delay
		Map.Entry<Double, String> highestAvgCarrierDelay = avgCarrierDelays.lastEntry();
		String carrier = highestAvgCarrierDelay.getValue();
		double highestAvgDelay = highestAvgCarrierDelay.getKey();

		context.write(new Text(carrier), new Text(String.valueOf(highestAvgDelay)));
	}
}
