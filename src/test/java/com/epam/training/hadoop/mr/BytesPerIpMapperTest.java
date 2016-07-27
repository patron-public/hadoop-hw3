package com.epam.training.hadoop.mr;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.epam.training.hadoop.custom.types.IntPairWritable;
import eu.bitwalker.useragentutils.Browser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;


public class BytesPerIpMapperTest {

	MapDriver<LongWritable, Text, IntWritable, IntPairWritable> mapDriver;
	List<String> samlpeLines;

	@Before
	public void setup() throws IOException {
		mapDriver = MapDriver.newMapDriver(new BytesPerIpMapper());
		String path = new File(getClass().getClassLoader().getResource("sample.txt").getFile()).getAbsolutePath();
		Path wiki_path = Paths.get(path, "");
		samlpeLines = Files.readAllLines(wiki_path, Charset.forName("UTF-8"));
	}

	@Test
	public void testMapperPass() throws IOException {

		mapDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(0)));
		mapDriver.withOutput(new IntWritable(147), new IntPairWritable(1,10529));
		
		mapDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(1)));
		mapDriver.withOutput(new IntWritable(32), new IntPairWritable(1,0));

		mapDriver.runTest();
	}

	@Test
	public void testMapperFail() throws IOException {
		
		mapDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(2)));
		mapDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(3)));

		List<Pair<IntWritable, IntPairWritable>> result = mapDriver.run();

		assertEquals(result.size(), 0);
	}
	
	@Test
	public void testMapperParseErrCounter() throws IOException {
		
		mapDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(2)));
		mapDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(3)));
		
		mapDriver.run();

		assertEquals("Expected 2 counter increment", 2,
				mapDriver.getCounters().findCounter(BytesPerIpMapper.MapperErrors.BYTES_PARSE_ERROR).getValue());
	}

	@Test
	public void testMapperClientCounter() throws IOException {
		
		mapDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(0)));
		mapDriver.withInput(new LongWritable(1), new Text(samlpeLines.get(1)));
		mapDriver.withInput(new LongWritable(2), new Text(samlpeLines.get(2)));
		mapDriver.run();

		assertEquals("Expected 2 counter increment", 1,
				mapDriver.getCounters().findCounter(Browser.IE8).getValue());
		assertEquals("Expected 0 counter increment", 1,
				mapDriver.getCounters().findCounter(BytesPerIpMapper.MapperErrors.BYTES_PARSE_ERROR).getValue());
	}

	
}
