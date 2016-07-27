package com.epam.training.hadoop.mr;

import com.epam.training.hadoop.custom.types.IntPairWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;


public class BytesPerIpMrTest {

    MapReduceDriver<LongWritable, Text, IntWritable, IntPairWritable, IntWritable, IntPairWritable> mapReduceDriver;
    List<String> samlpeLines;

    @Before
    public void setup() throws IOException {
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(new BytesPerIpMapper(), new BytesPerIpReducer());
        String path = new File(getClass().getClassLoader().getResource("sample.txt").getFile()).getAbsolutePath();
        Path wiki_path = Paths.get(path, "");
        samlpeLines = Files.readAllLines(wiki_path, Charset.forName("UTF-8"));
    }

    @Test
    public void testMRValidInput() throws IOException {
        mapReduceDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(0)));
        mapReduceDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(1)));
        mapReduceDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(4)));

        mapReduceDriver.withOutput(new Pair<IntWritable, IntPairWritable>(new IntWritable(32), new IntPairWritable(0, 0)));
        mapReduceDriver.withOutput(new Pair<IntWritable, IntPairWritable>(new IntWritable(147), new IntPairWritable(10530, 21060)));
        mapReduceDriver.withCounter(BytesPerIpMapper.MapperErrors.BYTES_PARSE_ERROR, 0);

        mapReduceDriver.runTest();
    }

    @Test
    public void testMRInvalidInput() throws IOException {

        mapReduceDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(2)));
        mapReduceDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(3)));

        mapReduceDriver.withCounter(BytesPerIpMapper.MapperErrors.BYTES_PARSE_ERROR, 2);
        mapReduceDriver.runTest();
    }


}
