package com.epam.training.hadoop.mr;

import com.epam.training.hadoop.custom.types.IntPairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BytesPerIpDriver extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Use hdmr <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = getConf();
        conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        Job job = Job.getInstance(conf);
        job.setJarByClass(BytesPerIpDriver.class);
        job.setJobName("AvgBytesJob");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setCompressOutput(job, true);


        job.setMapperClass(BytesPerIpMapper.class);
        job.setCombinerClass(BytesPerIpCombiner.class);
        job.setReducerClass(BytesPerIpReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntPairWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new BytesPerIpDriver(), args);
        System.exit(res);
    }
}
