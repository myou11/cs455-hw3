package cs455.hadoop.getInput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GetInputJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job getInputJob = Job.getInstance(conf, "getInput");
            // Current class.
            getInputJob.setJarByClass(GetInputJob.class);
            // Mapper
            getInputJob.setMapperClass(GetInputMapper.class);
            // Combiner. We use the reducer as the combiner in this case.
            // TODO: COMMENTED OUT COMBINER FOR THIS JOB BECAUSE IT DOESN'T REALLY MAKE SENSE?
            getInputJob.setCombinerClass(GetInputCombiner.class);

            // 1 Reducer for each question
            getInputJob.setNumReduceTasks(7);

            // Custom Partitioner to get inputs to the right Reducer
            getInputJob.setPartitionerClass(QuestionPartitioner.class);

            // Reducer
            getInputJob.setReducerClass(GetInputReducer.class);
            // Outputs from the Mapper.
            getInputJob.setMapOutputKeyClass(Text.class);
            getInputJob.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            getInputJob.setOutputKeyClass(Text.class);
            getInputJob.setOutputValueClass(Text.class);
            // path to input in HDFS
			// TODO: IF MULTIPLE INPUTS DOES NOT WORK, UNCOMMENT THIS LINE AND DECREMENT ARGS BY 1
            //FileInputFormat.addInputPath(getInputJob, new Path(args[0]));
			MultipleInputs.addInputPath(getInputJob, new Path(args[0]), TextInputFormat.class, GetInputMapper.class);
			MultipleInputs.addInputPath(getInputJob, new Path(args[1]), TextInputFormat.class, GetPlaneDataMapper.class);
            // path to output in HDFS
            FileOutputFormat.setOutputPath(getInputJob, new Path(args[2]));
            // Block until the job is completed.
            if (getInputJob.waitForCompletion(true)) {
                Job delayJob = Job.getInstance(conf, "Best/worst time to fly to minimize/maximize delays");
                delayJob.setJarByClass(GetInputJob.class);

                delayJob.setMapperClass(DelayMapper.class);

                delayJob.setNumReduceTasks(1);
                delayJob.setReducerClass(DelayReducer.class);

                // Sets Mapper and Reducer output key and value to Text
                delayJob.setOutputKeyClass(Text.class);
                delayJob.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(delayJob, new Path(args[3]));
                FileOutputFormat.setOutputPath(delayJob, new Path(args[4]));

				if (delayJob.waitForCompletion(true)) {
					Job busiestAirportsJob = Job.getInstance(conf, "Top 10 Busiest Airports");
					busiestAirportsJob.setJarByClass(GetInputJob.class);

					busiestAirportsJob.setMapperClass(TopTenAirportsMapper.class);

					// Have only 1 Reducer so we can get global top 10 busiest airports
					// instead of top 10 busiest airports for each Reducer
					busiestAirportsJob.setNumReduceTasks(1);
					busiestAirportsJob.setReducerClass(TopTenAirportsReducer.class);

					// Sort output in descending order so Reducer gets inputs from largest to smallest
					//busiestAirportsJob.setSortComparatorClass(LongWritable.DecreasingComparator.class);

					// Mapper and Reducer output types
					busiestAirportsJob.setOutputKeyClass(Text.class);
					busiestAirportsJob.setOutputValueClass(Text.class);

					FileInputFormat.addInputPath(busiestAirportsJob, new Path(args[5]));
					FileOutputFormat.setOutputPath(busiestAirportsJob, new Path(args[6]));

					if (busiestAirportsJob.waitForCompletion(true)) {
						Job topTenAirportsPerMonthJob = Job.getInstance(conf, "Best/worst time to fly to minimize/maximize delays");
						topTenAirportsPerMonthJob.setJarByClass(GetInputJob.class);

						topTenAirportsPerMonthJob.setMapperClass(BusiestAirportsPerMonthMapper.class);

						// Each Reducer gets a key for one month
						topTenAirportsPerMonthJob.setNumReduceTasks(12);
						topTenAirportsPerMonthJob.setPartitionerClass(BusiestAirportsPerMonthPartitioner.class);
						topTenAirportsPerMonthJob.setReducerClass(BusiestAirportsPerMonthReducer.class);

						// Sets Mapper and Reducer output key and value to Text
						topTenAirportsPerMonthJob.setOutputKeyClass(Text.class);
						topTenAirportsPerMonthJob.setOutputValueClass(Text.class);

						FileInputFormat.addInputPath(topTenAirportsPerMonthJob, new Path(args[7]));
						FileOutputFormat.setOutputPath(topTenAirportsPerMonthJob, new Path(args[8]));
						
						System.exit(topTenAirportsPerMonthJob.waitForCompletion(true) ? 0 : 1);
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
