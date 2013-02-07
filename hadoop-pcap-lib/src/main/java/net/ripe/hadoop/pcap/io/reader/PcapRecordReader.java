package net.ripe.hadoop.pcap.io.reader;

import java.io.DataInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;

import net.ripe.hadoop.pcap.PcapReader;
import net.ripe.hadoop.pcap.packet.Packet;

import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class PcapRecordReader extends RecordReader<LongWritable, ObjectWritable> {
    PcapReader pcapReader;
    Iterator<Packet> pcapReaderIterator;
    Seekable baseStream;
    DataInputStream stream;

    long packetCount = 0;
    long start, end;

    LongWritable key = null;
    ObjectWritable value = null;

    public PcapRecordReader(PcapReader pcapReader, long start, long end, Seekable baseStream, DataInputStream stream) throws IOException {
        this.pcapReader = pcapReader;
        this.baseStream = baseStream;
        this.stream = stream;
        this.start = start;
        this.end = end;

        pcapReaderIterator = pcapReader.iterator();
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (pcapReaderIterator.hasNext()) {
            key = new LongWritable(++packetCount);
            value = new ObjectWritable(pcapReaderIterator.next());
            return true;
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public ObjectWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end)
            return 0;
        return Math.min(1.0f, (getPos() - start) / (float) (end - start));
    }

    public long getPos() throws IOException {
        return baseStream.getPos();
    }
}