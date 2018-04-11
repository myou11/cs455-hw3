package cs455.hadoop.getInput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class QuestionPartitioner extends Partitioner<Text, Text> {

	// numReduceTasks will be the number of months (12)
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
		String[] question_category = key.toString().split(":");
		int questionNum = Integer.parseInt(question_category[0].substring(1,2));

		// Partition each month (key) to a reducer
		return questionNum % numReduceTasks;
    }
}
