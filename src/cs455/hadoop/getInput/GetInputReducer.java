package cs455.hadoop.getInput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GetInputReducer extends Reducer<Text, Text, Text, Text> {

	// Q5 - Do older planes have more delays
	//private HashMap<Integer, Long> flightYearDelays;
	//private HashMap<Integer, Long> flightYearCounts;
	//private int manufactureYear;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		//this.flightYearDelays = new HashMap<>();
		//this.flightYearCounts = new HashMap<>();
		//this.manufactureYear = manufactureYear
	}

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		System.out.println(key.toString());
		String[] keySplit = key.toString().split(":");

		String question = keySplit[0];

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

		if (question.equals("q3")) {
			long count = 0;
			for (Text val : values) {
				count += Long.parseLong(val.toString());
			}

            context.write(key, new Text(String.valueOf(count)));
		}

		if (question.equals("q6")) {
			String city = null;
			long count = 0;

			for (Text val : values) {
				String[] countOrCity = val.toString().split("_");
				if (countOrCity.length == 1) {
					city = val.toString();
				} else { // countOrCity has 2 elements which means its a count b/c we attached a dummy x to the counts
					count += Long.parseLong(countOrCity[0]);
				}
			}

			// There is a chance that there is no corresponding city to this airport.
			// In this case, splitting the value will never equal 1 because a city was
			// never sent. Therefore, we toss out the count for this airport by not
			// writing it to the Reducers since we cannot connect it back to a city.
			if (city != null) {
				// replace the airport with its designated city
				String question_city = "q6:" + city;

				// No need to append the dummy x to the end of the count anymore.
				// Send just the count as the value so TopTenAirportsMapper doesn't
				// have to parse out the extra dummy x.
				context.write(new Text(question_city), new Text(String.valueOf(count)));
			}
		}

		if (question.equals("q7")) {
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

		///*
		if (question.equals("q5")) {

			String tailNum = keySplit[1];

			// Key: flight year, Value: cumulative delay
			HashMap<Integer, Long> flightYearDelays = new HashMap<>();

			// Key: flight year, Value: number of delays
			HashMap<Integer, Long> flightYearCounts = new HashMap<>();

			// TODO: This should get assigned when looping through the values for a tailnum b/c I explicitly
			int manufactureYear = -1;

			for (Text val : values) {
				String[] year_arrDelay_count = val.toString().split("_");

				// This is handled differently than in the Combiner.
				// In the Reducer, we know we will get the manufacture year in the list of values for this tailNum.
				// Therefore, we can save it here and use it after we add up all the delays and counts for this tailNum.
				// Use the manufacture year after looping through all the values to distinguish between old and new planes at the time of the flight.
				if (year_arrDelay_count.length == 1) {
					// Only has the manufacture year b/c it came from the plane-data mapper
					manufactureYear = Integer.parseInt(year_arrDelay_count[0]);
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

			// Since this is the reducer, and I wrote a different combiner for this question,
			// it has all values for this tailNum at this point.
			// However, it is not guaranteed that the manufacture year was set.
			// There could be many tailNums in the main dataset that do not have corresponding entries in the plane-data dataset.
			// In these cases, the manufacture year will still be -1 at this point.
			// Throw out the info for this tailnum (the current key we are reducing). Can't know if we should throw out the data
			// for this tailNum until we get to this point, b/c at this point we know whether or not there was a manufacture year.
			if (manufactureYear == -1)
				// Returning for this key essentially throws out the data we collected for it
				return;

			for (Map.Entry<Integer, Long> entry : flightYearDelays.entrySet()) {
				int flightYear = entry.getKey();
				long delay = entry.getValue();
				// Get the corresponding count for the same year
				long count = flightYearCounts.get(flightYear);

				String arrDelay_count = String.format("%d_%d", delay, count);

				// old plane delays
				if ((flightYear - manufactureYear) > 20) {
					//String question_year_manufactureYear = String.format("q5:%d:%d", flightYear, manufactureYear);
					//context.write(new Text(question_year_manufactureYear), new Text(arrDelay_count));
					context.write(new Text("q5:old"), new Text(arrDelay_count));
				}

				// new plane delays
				else {
					//String question_year_manufactureYear = String.format("q5:%d:%d", flightYear, manufactureYear);
					//context.write(new Text(question_year_manufactureYear), new Text(arrDelay_count));
					context.write(new Text("q5:new"), new Text(arrDelay_count));
				}
			}
		}
	}
}
