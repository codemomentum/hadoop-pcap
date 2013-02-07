package net.ripe.hadoop.pcap.io.reader;

import net.ripe.hadoop.pcap.io.PcapInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;


public class CombinePcapRecordReader extends RecordReader<LongWritable, ObjectWritable> {

    private PcapRecordReader recordReader;

    public CombinePcapRecordReader(CombineFileSplit split,TaskAttemptContext context,Integer index) throws IOException {
        Path path = split.getPath(index);
        long start = 0L;
        long length = split.getLength(index);
        recordReader = PcapInputFormat.initPcapRecordReader(path, start, length, context.getConfiguration());
    }

    @Override
    public void close() throws IOException {
        recordReader.close();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return recordReader.nextKeyValue();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return recordReader.getCurrentKey();
    }

    @Override
    public ObjectWritable getCurrentValue() throws IOException, InterruptedException {
        return recordReader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return recordReader.getProgress();
    }

    public long getPos() throws IOException {
        return recordReader.getPos();
    }
}
