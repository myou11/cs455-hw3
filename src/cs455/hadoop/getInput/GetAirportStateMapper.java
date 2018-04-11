package cs455.hadoop.getInput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GetAirportStateMapper extends Mapper<LongWritable, Text, Text, Text> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // All string fields in the csv are surrounded by quotes, so replace them with empty strings
        String[] dataRow = value.toString().split(",");
        String firstRowCheck = dataRow[0].replace("\"", "");

        if (firstRowCheck.equals("iata"))
            return;

        // dataRow[0]: airport code
        String airport = dataRow[0].replace("\"", "");
        // dataRow[3]: city
        String city = dataRow[2].replace("\"", "");
        // some cities have random whitespace at the beginning? which is throwing off my parsing of key-value pairs, so I remove them
        city = city.replace(" ", "-");
        if (!airport.equals("NA") && !city.equals("NA")) {
            String question_airport = "q6:" + airport;

            // prepend "city_" to the city name so we can distinguish between outputs from this mapper vs
            // outputs from the main data mapper.
            // main data mapper will be sending <q6:airport, count>, so it would be hard to distinguish
            // which output came from which file just by length of the value (of the key-value pair)
            context.write(new Text(question_airport), new Text(city));
        }
	}
}
