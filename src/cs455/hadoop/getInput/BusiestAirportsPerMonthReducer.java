package cs455.hadoop.getInput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives TODO: add documentation here
 */
public class BusiestAirportsPerMonthReducer extends Reducer<Text, Text, Text, Text> {

	private String month;
	private TreeMap<Long, String> airportCounts;

	@Override
	protected void setup(Context context) {
		this.month = "";
		this.airportCounts = new TreeMap(Collections.reverseOrder());
	}

	/*
	 * Each Reducer will only receive one key (month) b/c I set the num of reducers to the number of months
	 */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String[] question_month = key.toString().split(":");
		this.month = question_month[1];

		// Go through the airport_counts for this month and put the counts and airports into a TreeMap
		// to order them so we can find the top 100 in the cleanup
		for (Text val : values) {
			String[] airport_count = val.toString().split("_");
			String airport = airport_count[0];
			long count = Long.parseLong(airport_count[1]);

			// There is a possibility that the count for an airport is the same
			// as the count for another airport already in the map.
			// I am letting the airport already in the map with the same count as
			// the current airport and count, to be overridden in favor of the new airport.
			// This new airport just takes the same ranking as the previous airport did
			airportCounts.put(count, airport);
		}
    }

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text("month,airport,count"), new Text());

		int counter = 0;
		for (Map.Entry<Long, String> entry : airportCounts.entrySet()) {
			if (counter == 100)
				break;
			String airport = entry.getValue();
			long count = entry.getKey();

			//String formattedAirportCount = String.format("Airport: %s, Times visited: %d", airport, count);
			//String airport_count = String.format("%s_%d", airport, count);
			
			//context.write(new Text(this.month), new Text(formattedAirportCount));
			//context.write(new Text(airport), new Text(String.valueOf(count)));

			String month_airport_count_csvFormat = String.format("%s,%s,%d", this.month, airport, count);
			context.write(new Text(month_airport_count_csvFormat), new Text());

			counter++;
		}
	}
}
