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
public class TopTenAirportsReducer extends Reducer<Text, Text, Text, Text> {

	// Q3 - busiest airports
	private TreeMap<Long, String> busiestAirportsBefore1998;
	private TreeMap<Long, String> busiestAirportsOnOrAfter1998;

	// Q6 - cities with most weather-related delays
	private TreeMap<Long, String> cityWeatherDelays;

	@Override
	protected void setup(Context context) {
		busiestAirportsBefore1998 = new TreeMap<>();
		busiestAirportsOnOrAfter1998 = new TreeMap<>();
		cityWeatherDelays = new TreeMap<>();
	}

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Will have 3 elements if it is related to question 3, else 2 elements if related to question 6
		String[] question_airport_before1998 = key.toString().split(":");
		String question = question_airport_before1998[0];

		if (question.equals("q3")) {
			String airport = question_airport_before1998[1];
			int before1998 = Integer.parseInt(question_airport_before1998[2]);
			
			for (Text val : values) {
				long count = Long.parseLong(val.toString());

				if (before1998 == 1)
					busiestAirportsBefore1998.put(count, airport);
				else
					busiestAirportsOnOrAfter1998.put(count, airport);
			}
		}

		else if (question.equals("q6")) {
			// question_airport_before1998 will actually be a question_city for Q6
			// question_airport_before1998[0] = question_city[0] = question
			// question_airport_before1998[1] = question_city[1] = city
			String city = question_airport_before1998[1];
			for (Text val : values) {
				long count = Long.parseLong(val.toString());

				cityWeatherDelays.put(count, city);
			}
		}
    }

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// Q3 - Top 10 busiest airports before 1998

		TreeMap<Long, String> reversedBusiestAirportsBefore1998 = new TreeMap(Collections.reverseOrder());
		reversedBusiestAirportsBefore1998.putAll(busiestAirportsBefore1998);

		context.write(new Text("Top 10 busiest airports before 1998"), new Text());

		int counter1 = 0;
		for (Map.Entry<Long, String> count_airport : reversedBusiestAirportsBefore1998.entrySet()) {
			if (counter1 == 10)
				break;

			String airport = count_airport.getValue();
			long count = count_airport.getKey();

			context.write(new Text(airport), new Text(String.valueOf(count)));

			counter1++;
		}

		// Q3 - Top 10 busiest airports on or after 1998

		TreeMap<Long, String> reversedBusiestAirportsOnOrAfter1998= new TreeMap(Collections.reverseOrder());
		reversedBusiestAirportsOnOrAfter1998.putAll(busiestAirportsOnOrAfter1998);

		context.write(new Text("\nTop 10 busiest airports on or after 1998"), new Text());

		int counter2 = 0;
		for (Map.Entry<Long, String> count_airport : reversedBusiestAirportsOnOrAfter1998.entrySet()) {
			if (counter2 == 10)
				break;

			String airport = count_airport.getValue();
			long count = count_airport.getKey();

			context.write(new Text(airport), new Text(String.valueOf(count)));

			counter2++;
		}

		// TODO: NEED TO AGGREGATE THE AIRPORT COUNTS AMONGST CITIES NOT JUST AIRPORTS AS THEY ARE
		// TODO: B/C MULTIPLE AIRPORTS COULD BE IN ONE CITY
		// Q6 - Top 10 airports with the most weather-related delays

		TreeMap<Long, String> reversedCityWeatherDelays = new TreeMap(Collections.reverseOrder());
		reversedCityWeatherDelays.putAll(cityWeatherDelays);

		context.write(new Text("\nTop 10 cities with the most weather-related delays"), new Text());

		int counter3 = 0;
		for (Map.Entry<Long, String> count_city : reversedCityWeatherDelays.entrySet()) {
			if (counter3 == 10)
				break;

			String city = count_city.getValue();
			long count = count_city.getKey();

			context.write(new Text(city), new Text(String.valueOf(count)));

			counter3++;
		}
	}
}
