package com.opstty.q6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class OldestTree {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: oldesttree [input] [output]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "oldesttree");
        job.setJarByClass(OldestTree.class);
        job.setMapperClass(OldestTreeMapper.class);
        job.setReducerClass(OldestTreeReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // Set path
        Path inputFilePath = new Path(args[0]);
        Path outputFilePath = new Path(args[1]);
        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);
        // If output already exists, delete it
        FileSystem fs = FileSystem.newInstance(conf);
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

