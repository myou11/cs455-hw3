package cs455.hadoop.getInput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GetPlaneDataMapper extends Mapper<LongWritable, Text, Text, Text> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataRow = value.toString().split(",");

        if (dataRow.length < 9 || dataRow[0].equals("tailnum"))
            return;

        // dataRow[8]: tailNum ('None' if not present)
        if (!dataRow[0].equals("NA") && !dataRow[8].equals("None")) {
            String tailNum = "q5:" + dataRow[0];
            String year = dataRow[8];

            context.write(new Text(tailNum), new Text(year));
        }
	}
}
