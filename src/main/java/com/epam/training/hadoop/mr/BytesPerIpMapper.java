package com.epam.training.hadoop.mr;

import com.epam.training.hadoop.custom.types.IntPairWritable;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BytesPerIpMapper extends Mapper<LongWritable, Text, IntWritable, IntPairWritable> {

    private static final String DELIMITER_SPACE = " ";
    private static final String DELIMITER_QUOTED_SPACE = "\" \"";
    private static final String QUOTES_AND_SPACE = "\" ";

    public enum MapperErrors {
        BYTES_PARSE_ERROR
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();

            int ip = Integer.valueOf(line.substring(2, line.indexOf(DELIMITER_SPACE)));

            // end of request quotes + 1 space + 3char-resp-code + space
            int bytesStartPos = line.indexOf(QUOTES_AND_SPACE) + 6;

            if (bytesStartPos > 6) {
                String bytesTransfered = line.substring(bytesStartPos, line.indexOf(DELIMITER_SPACE, bytesStartPos));
                int clientStartPos = line.indexOf(DELIMITER_QUOTED_SPACE) + 3;

                int bytes = 0;
                if (!"-".equals(bytesTransfered)) {
                    bytes = Integer.valueOf(bytesTransfered);
                }

                String clientString = line.substring(clientStartPos, line.length());
                context.getCounter(UserAgent.parseUserAgentString(clientString).getBrowser()).increment(1);

                context.write(new IntWritable(ip), new IntPairWritable(1, bytes));
            }
        } catch (NumberFormatException e) {
            context.getCounter(MapperErrors.BYTES_PARSE_ERROR).increment(1);
        }

    }

}
