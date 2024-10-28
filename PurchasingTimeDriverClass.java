package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PurchasingTimeDriverClass {

    public static void main(String[] args) throws Exception {
        // Check that enough arguments are provided
        // if (args.length != 3) {
        //     System.err.println("Usage: EngagedUsersDriverClass <input path> <output base path> <temp output path>");
        //     System.exit(-1);
        // }

        Configuration conf = new Configuration();

        // ===== First Job: Count Activities per User =====
        Job job = Job.getInstance(conf, "Find Most Popular Purchasing Hours");
        job.setJarByClass(PurchasingTimeDriverClass.class);

        // Set Mapper and Reducer for the first job
        job.setMapperClass(ProductPurchasesMapper.class); // Use EngagedUsersMapper
        job.setReducerClass(ProductPurchasesReducer.class); // Use EngagedUsersReducer

        // Set output key and value types for the first job
        job.setOutputKeyClass(Text.class);  // Change to Text, as emitted by the mapper
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths for the first job
        FileInputFormat.addInputPath(job, new Path(args[1]));  // Input path (from args[1])
        FileOutputFormat.setOutputPath(job, new Path(args[2]));  // Temp output for intermediate results
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
