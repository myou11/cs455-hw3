package cs455.hadoop.getInput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GetInputCombiner extends Reducer<Text, Text, Text, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    	// Below are some different actions to take depending on the question being processed.
		// Some of these actions for questions are the same as in the Reducer, but we need them in
		// the Combiner b/c if the Combiner is run and we don't check for some of the questions,
		// the questions will not get passed on to the Reducer.

		String[] keySplit = key.toString().split(":");

		String question = keySplit[0];

		// Same functionality as in Reducer
		if (question.equals("q1q2") || question.equals("q4")) {
			long cumulativeDelay = 0;
			long cumulativeNumDelays = 0;

			for (Text val : values) {
				String[] delay_numDelays = val.toString().split("_");
				long delay = Long.parseLong(delay_numDelays[0]);
				long numDelays = Long.parseLong(delay_numDelays[1]);

				cumulativeDelay += delay;
				cumulativeNumDelays += numDelays;
			}

			// NOTE: CAN'T HAVE DO DIVISION IN THIS CLASS B/C IT IS USED AS A COMBINER
			// COMBINERS MUST BE COMMUTATIVE AND ASSOCIATIVE (DIVISION IS NOT COMMUTATIVE)
			//cumulativeDelay /= numDelays;

			// Instead of dividing cumulativeDelay here, wait until the cleanup function
			// just keep track of total number of delays
			String delay_numDelays = String.format("%d_%d", cumulativeDelay, cumulativeNumDelays);

			context.write(key, new Text(delay_numDelays));
		}

		// Same functionality as in Reducer
		else if (question.equals("q3") || question.equals("q6")) {
			long count = 0;
			for (Text val : values) {
				count += Long.parseLong(val.toString());
			}
			context.write(key, new Text(String.valueOf(count)));
		}

		// Only this question's Combiner functionality is different from its Reducer counterpart
		else if (question.equals("q5")) {

			String tailNum = keySplit[1];

			// Key: flight year, Value: cumulative delay
			HashMap<Integer, Long> flightYearDelays = new HashMap<>();

			// Key: flight year, Value: number of delays
			HashMap<Integer, Long> flightYearCounts = new HashMap<>();

			for (Text val : values) {
				String[] year_arrDelay_count = val.toString().split("_");

				// Since this is the Combiner, we might or might not get have the manufacture year as a value.
				// Therefore, we can only write it to the Reducer if we see it.
				// Can't cache the manufacture year and use it to distinguish between old and new planes b/c
				// the manufacture year is only going to be output from one of the mappers that is operating on the plane data
				// and the Combiner might or might not run for each Mapper.
				if (year_arrDelay_count.length == 1) {
					context.write(key, val);
					continue;
				}

				int year = Integer.parseInt(year_arrDelay_count[0]);
				long arrDelay = Long.parseLong(year_arrDelay_count[1]);
				long count = Long.parseLong(year_arrDelay_count[2]);

				// Is thread safe because only one reduce task is operating on a specific tailnum at one time
				if (flightYearDelays.putIfAbsent(year, arrDelay) != null) {
					long cumulativeArrDelay = flightYearDelays.get(year) + arrDelay;
					flightYearDelays.put(year, cumulativeArrDelay);
				}

				if (flightYearCounts.putIfAbsent(year, count) != null) {
					long cumulativeCount = flightYearCounts.get(year) + count;
					flightYearCounts.put(year, cumulativeCount);
				}
			}

			// Since this is the Combiner, which has different slightly different behavior for Q5,
			// it has the cumulative arrDelay and counts for the tailNums it mapped.
			// There are probably arrDelay and counts for some of these same tailNums in other Mappers.
			// The Combiner just helps to aggregate some of the arrDelays and counts before they get
			// to the Reducer.
			for (Map.Entry<Integer, Long> entry : flightYearDelays.entrySet()) {
				int flightYear = entry.getKey();
				long arrDelay = entry.getValue();
				// Get the corresponding count for the same year
				long count = flightYearCounts.get(flightYear);

				String year_arrDelay_count = String.format("%d_%d_%d", flightYear, arrDelay, count);

				// We want to write the question_tailNum as the key and the year_arrDelay_count as the value
				// so the Reducer can still parse it out correctly.
				context.write(key, new Text(year_arrDelay_count));
			}
		}

		// Same functionality as in Reducer
		else if (question.equals("q7")) {
			HashMap<String, Long> airportCounts = new HashMap<>();

			// Each Reducer get a set of months, there are only 12 possibilities.
			// Each month key comes with a list of airport_counts for that month
			// (basically the number of times an airport was visited in that month).
			// This loop goes through each airport_count (val) and splits it up into an airport and the count.
			// A hashmap is used to sum up the counts for that airport.
			// This map will have the cumulative visits to that airport for the current month (key) at the end.
			for (Text val : values) {
				String[] airport_count = val.toString().split("_");
				String airport = airport_count[0];
				Long count = Long.parseLong(airport_count[1]);

				// used Long obj so that cumulativeCount could be null
				// if the element didn't exist yet, putIfAbsent puts it and returns null
				// can't store null in a primitive long
				Long cumulativeCount = airportCounts.putIfAbsent(airport, count);

				// airport is already in the map, cumulative count now contains the current count for that airport
				if (cumulativeCount != null) {
					airportCounts.put(airport, cumulativeCount + count);
				}
			}

			// Output the counts for each airport for the month (key)
			for (Map.Entry<String, Long> entry : airportCounts.entrySet()) {
				String airport = entry.getKey();
				long count = entry.getValue();
				String airport_count = String.format("%s_%d", airport, count);

				context.write(key, new Text(airport_count));
			}
		}
	}
}
