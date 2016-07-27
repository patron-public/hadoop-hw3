package com.epam.training.hadoop.mr;

import com.epam.training.hadoop.custom.types.IntPairWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BytesPerIpReducer extends Reducer<IntWritable, IntPairWritable, IntWritable, IntPairWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<IntPairWritable> values, Context context)
            throws IOException, InterruptedException {
        int total = 0;
        int count = 0;

        for (IntPairWritable value : values) {
            count += value.getFirst();
            total += value.getSecond();
        }

        int avg = total / count;
        context.write(key, new IntPairWritable(avg, total));
    }

}
