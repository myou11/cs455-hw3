package cs455.hadoop.busiestAirports;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * This is the main class. Hadoop will invoke the main method of this class.
 */
public class BusiestAirportsJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job = Job.getInstance(conf, "airlineAnalysis");
            // Current class.
            job.setJarByClass(BusiestAirportsJob.class);
            // Mapper
            job.setMapperClass(BusiestAirportsMapper.class);
            // Combiner. We use the reducer as the combiner in this case.
            job.setCombinerClass(BusiestAirportsReducer.class);
            // Reducer
            job.setReducerClass(BusiestAirportsReducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // Block until the job is completed.
            //System.exit(job.waitForCompletion(true) ? 0 : 1);
			
			if (job.waitForCompletion(true)) {
				Job job2 = Job.getInstance(conf, "Top 10 Busiest Airports");
				job2.setJarByClass(BusiestAirportsJob.class);

				job2.setMapperClass(SortBusiestAirportsMapper.class);
				
				// Have only 1 Reducer so we can get global top 10 busiest airports
				// instead of top 10 busiest airports for each Reducer
				job2.setNumReduceTasks(1);
				job2.setReducerClass(TopTenBusiestAirportsReducer.class);

				// Sort output in descending order so Reducer gets inputs from largest to smallest
				job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

				// Mapper Outputs
				job2.setMapOutputKeyClass(LongWritable.class);
				job2.setMapOutputValueClass(Text.class);

				// Reducer Outputs
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(LongWritable.class);

				FileInputFormat.addInputPath(job2, new Path(args[2]));
				FileOutputFormat.setOutputPath(job2, new Path(args[3]));

				System.exit(job2.waitForCompletion(true) ? 0 : 1);
			}
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }

    }
}
