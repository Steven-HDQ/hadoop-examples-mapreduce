package com.opstty.q7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MostTree {
    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: mosttree [input] [output]");
            System.exit(2);
        }
        Job job1 = Job.getInstance(conf1);
        job1.setJarByClass(MostTree.class);
        job1.setMapperClass(MostTreeMapper1.class);
        job1.setReducerClass(MostTreeReducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        // Set path
        Path inputFilePath1 = new Path(args[0]);
        Path outputFilePath1 = new Path("FirstMapper");
        FileInputFormat.addInputPath(job1, inputFilePath1);
        FileOutputFormat.setOutputPath(job1, outputFilePath1);
        // If output already exists, delete it
        FileSystem fs1 = FileSystem.newInstance(conf1);
        if (fs1.exists(outputFilePath1)) {
            fs1.delete(outputFilePath1, true);
        }
        job1.waitForCompletion(true);

        //job2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2,"mosttree");
        job2.setJarByClass(MostTree.class);
        job2.setMapperClass(MostTreeMapper2.class);
        job2.setReducerClass(MostTreeReducer2.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(MyWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        // Set path
        Path inputFilePath2 = outputFilePath1;
        Path outputFilePath2 = new Path(args[1]);
        FileInputFormat.addInputPath(job2, inputFilePath2);
        FileOutputFormat.setOutputPath(job2, outputFilePath2);
        // If output already exists, delete it
        FileSystem fs2 = FileSystem.newInstance(conf2);
        if (fs2.exists(outputFilePath2)) {
            fs2.delete(outputFilePath2, true);
        }

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

