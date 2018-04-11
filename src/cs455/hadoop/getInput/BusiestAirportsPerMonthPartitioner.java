package cs455.hadoop.getInput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class BusiestAirportsPerMonthPartitioner extends Partitioner<Text, Text> {

	// Only used for Q7, to partition outputs from the Q7 Mapper (BusiestAirportsPerMonthMapper)
	// to the Q7 Reducer (BusiestAirportsPerMonthReducer)
	// numReduceTasks will be the number of months (12)
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
		String[] question_month = key.toString().split(":");
		int month = Integer.parseInt(question_month[1]);

		// Partition each month (key) to a reducer
		return month % numReduceTasks;
    }
}
