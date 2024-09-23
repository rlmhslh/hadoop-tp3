package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

public class RelationshipRecordReader extends RecordReader<Text, Relationship> {

    private FSDataInputStream inputStream;
    private Text key = new Text();
    private Relationship value = new Relationship();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        Path file = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) split).getPath();
        inputStream = file.getFileSystem(context.getConfiguration()).open(file);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        String line = inputStream.readLine();
        if (line == null) {
            return false;
        }

        String[] parts = line.split("\t");
        if (parts.length == 2) {
            key.set(parts[0]);
            value.setId1(parts[0]);
            value.setId2(parts[1]);
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() {
        return key;
    }

    @Override
    public Relationship getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        return 0.0f;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
