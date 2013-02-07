package net.ripe.hadoop.pcap.io;

import net.ripe.hadoop.pcap.io.reader.CombinePcapRecordReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

public class CombinePcapInputFormat extends CombineFileInputFormat<LongWritable, ObjectWritable> {
    @Override
    public RecordReader<LongWritable, ObjectWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader((CombineFileSplit) split, context, CombinePcapRecordReader.class);
    }
}

