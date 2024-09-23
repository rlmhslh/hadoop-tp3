package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;

public class RelationshipInputFormat extends FileInputFormat<Text, Relationship> {

    @Override
    public RecordReader<Text, Relationship> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new RelationshipRecordReader(); // Le type doit être RecordReader<Text, Relationship>
    }

}
