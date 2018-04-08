package cs455.hadoop.getInput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GetInputJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job = Job.getInstance(conf, "getInput");
            // Current class.
            job.setJarByClass(GetInputJob.class);
            // Mapper
            job.setMapperClass(GetInputMapper.class);
            // Combiner. We use the reducer as the combiner in this case.
            // TODO: COMMENTED OUT COMBINER FOR THIS JOB BECAUSE IT DOESN'T REALLY MAKE SENSE?
            job.setCombinerClass(GetInputReducer.class);
            // Reducer
            job.setReducerClass(GetInputReducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // Block until the job is completed.
            if (job.waitForCompletion(true)) {
                Job delayJob = Job.getInstance(conf, "Best/worst time to fly to minimize/maximize delays");
                delayJob.setJarByClass(GetInputJob.class);

                delayJob.setMapperClass(DelayMapper.class);

                delayJob.setNumReduceTasks(1);
                delayJob.setReducerClass(DelayReducer.class);

                // Sets Mapper and Reducer output key and value to Text
                delayJob.setOutputKeyClass(Text.class);
                delayJob.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(delayJob, new Path(args[2]));
                FileOutputFormat.setOutputPath(delayJob, new Path(args[3]));

				if (delayJob.waitForCompletion(true)) {
					Job busiestAirportsJob = Job.getInstance(conf, "Top 10 Busiest Airports");
					busiestAirportsJob.setJarByClass(GetInputJob.class);

					busiestAirportsJob.setMapperClass(SortBusiestAirportsMapper.class);

					// Have only 1 Reducer so we can get global top 10 busiest airports
					// instead of top 10 busiest airports for each Reducer
					busiestAirportsJob.setNumReduceTasks(1);
					busiestAirportsJob.setReducerClass(TopTenBusiestAirportsReducer.class);

					// Sort output in descending order so Reducer gets inputs from largest to smallest
					busiestAirportsJob.setSortComparatorClass(LongWritable.DecreasingComparator.class);

					// Mapper outputs
					busiestAirportsJob.setMapOutputKeyClass(LongWritable.class);
					busiestAirportsJob.setMapOutputValueClass(Text.class);

					// Reducer outputs
					busiestAirportsJob.setOutputKeyClass(Text.class);
					busiestAirportsJob.setOutputValueClass(LongWritable.class);

					FileInputFormat.addInputPath(busiestAirportsJob, new Path(args[4]));
					FileOutputFormat.setOutputPath(busiestAirportsJob, new Path(args[5]));

					if (busiestAirportsJob.waitForCompletion(true)) {
						Job carrierDelayJob = Job.getInstance(conf, "Carrier Delays");
						carrierDelayJob.setJarByClass(GetInputJob.class);

						carrierDelayJob.setMapperClass(CarrierDelayMapper.class);

						carrierDelayJob.setNumReduceTasks(1);
						carrierDelayJob.setReducerClass(CarrierDelayReducer.class);

						carrierDelayJob.setOutputKeyClass(Text.class);
						carrierDelayJob.setOutputValueClass(Text.class);

						FileInputFormat.addInputPath(carrierDelayJob, new Path(args[6]));
						FileOutputFormat.setOutputPath(carrierDelayJob, new Path(args[7]));

						System.exit(carrierDelayJob.waitForCompletion(true) ? 0 : 1);
					}
				}
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
