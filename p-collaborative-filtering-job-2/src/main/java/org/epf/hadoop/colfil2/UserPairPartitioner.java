package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class UserPairPartitioner extends Partitioner<UserPair, IntWritable> {
    @Override
    public int getPartition(UserPair key, IntWritable value, int numPartitions) {
        return (key.getFirstUser().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
