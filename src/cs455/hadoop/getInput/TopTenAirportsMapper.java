package cs455.hadoop.getInput;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.HashMap;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class TopTenAirportsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private HashMap<String, String> airportCities;

    private Logger logger = Logger.getLogger(TopTenAirportsMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.airportCities = new HashMap<>();

        // Create a map of each airport code to its state with the airports.csv file in the distributed cache
        /*
        URI[] files = context.getCacheFiles();
        for (URI file : files) {
            if (file.getPath().contains("airports.csv")) {
                Path path = new Path(file);
                BufferedReader bf = new BufferedReader(new FileReader(path.getName()));
                String inputLine = "";
                while ((inputLine = bf.readLine()) != null) {
                    String[] airport_fields = inputLine.split(",");
                    String airport = airport_fields[0];
                    String state = airport_fields[3];

                    airportCities.put(airport, state);
                }
            }
        }
        */
        ///*
        /*Path path = null;
        try {
            if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
                URI cachedFileURI = context.getCacheFiles()[0];

                if (cachedFileURI != null) {
                    path = new Path(cachedFileURI.toString());
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(
                                    new FileInputStream(path.toString())
                            )
                    );
                    String inputLine = "";
                    while ((inputLine = br.readLine()) != null) {
                        isFileBeingRead = "true";
                        String[] airport_fields = inputLine.split(",");

                        // The airport and state are surrounded by quotes in the csv file, so remove them.
                        // The airports in the main dataset don't have quotes around them, so they
                        // will never find the airports in the map if we keep the quotes.
                        String airport = airport_fields[0].replace("\"", "");
                        logger.info("airport from csv is: " + airport);
                        String state = airport_fields[3].replace("\"", "");
                        logger.info("state from csv is: " + airport);

                        airportCities.put(airport, state);
                    }

                    br.close();
                } else {
                    //System.exit(1);
                }
            } else {
                //System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        */
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // turn line into a string
        // parse the output from the previous job; key-value separated by whitespace; regex to split on whitespace
        String[] dataRow = value.toString().split("\\s+");

		// key
		String question_key = dataRow[0];

		// Both questions 3 and 6 have a question_key, value (number of visits) pair

        // value
        String count = dataRow[1];

		// Question 6 requires us to find the cities with the most weather-related delays.
        // The key will be in the form of <q6:city, count> if the question is 6.
        // Otherwise, the key will be in the form <q3:airport:before1998, count> if the question is 3.

        // This is will only be written for question 3, not 6
		context.write(new Text(question_key), new Text(count));
    }
}
