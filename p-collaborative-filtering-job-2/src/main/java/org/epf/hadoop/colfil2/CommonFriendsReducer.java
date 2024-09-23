package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.util.Iterator;

public class CommonFriendsReducer extends Reducer<UserPair, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(UserPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int commonRelationsCount = 0;
        boolean hasDirectConnection = false;

        // Parcourir les valeurs associées à la clé (la paire d'utilisateurs)
        Iterator<IntWritable> iter = values.iterator();
        while (iter.hasNext()) {
            IntWritable value = iter.next();
            if (value.get() == 1) {
                commonRelationsCount++;  // Incrémenter si c'est une relation commune
            } else if (value.get() == 0) {
                hasDirectConnection = true;  // Marquer si une relation directe existe
            }
        }

        // Ne pas émettre la paire si elle a une connexion directe ou si elle n'a pas de relation commune
        if (!hasDirectConnection && commonRelationsCount > 0) {
            context.write(new Text(key.toString()), new IntWritable(commonRelationsCount));
        }
    }
}
