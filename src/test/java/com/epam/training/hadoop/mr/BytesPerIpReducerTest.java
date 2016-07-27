package com.epam.training.hadoop.mr;

import com.epam.training.hadoop.custom.types.IntPairWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BytesPerIpReducerTest {

	ReduceDriver<IntWritable, IntPairWritable, IntWritable, IntPairWritable> reduceDriver;

	@Before
	public void setup() throws IOException {
		reduceDriver = ReduceDriver.newReduceDriver(new BytesPerIpReducer());
	}

	@Test
	public void testReducerMath() throws IOException {
		List<IntPairWritable> values1 = new ArrayList<IntPairWritable>();

		values1.add(new IntPairWritable(1,1));
		values1.add(new IntPairWritable(1,2));
		values1.add(new IntPairWritable(1,6));

		List<IntPairWritable> values2 = new ArrayList<IntPairWritable>();

		values2.add(new IntPairWritable(2,10));
		values2.add(new IntPairWritable(3,15));
		values2.add(new IntPairWritable(1,5));

		reduceDriver.withInput(new IntWritable(1), values1);
		reduceDriver.withInput(new IntWritable(2), values2);

		reduceDriver.withOutput(new IntWritable(1), new IntPairWritable(3, 9));
		reduceDriver.withOutput(new IntWritable(2), new IntPairWritable(5, 30));

		reduceDriver.runTest();

	}

}
