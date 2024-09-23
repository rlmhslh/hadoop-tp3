package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
public class CommonFriendsMapper extends Mapper<LongWritable, Text, UserPair, IntWritable> {

    private IntWritable one = new IntWritable(1);
    private IntWritable zero = new IntWritable(0);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Récupérer la ligne du fichier d'entrée
        String line = value.toString();
        String[] parts = line.split("\t");

        String userId = parts[0];  // ID de l'utilisateur
        String[] relations = parts[1].split(",");  // Liste des relations (utilisateurs liés)

        Set<String> userRelations = new HashSet<>();
        for (String relation : relations) {
            userRelations.add(relation);
        }

        // Créer les paires d'utilisateurs et les relations communes
        for (String relation : userRelations) {
            for (String otherRelation : userRelations) {
                if (!relation.equals(otherRelation)) {
                    UserPair pair = new UserPair(relation, otherRelation);
                    context.write(pair, one);  // Émettre la paire avec un "1" pour une relation commune
                }
            }

            // Propager l'information de connexion directe pour ne pas l'inclure dans le résultat
            for (String otherRelation : userRelations) {
                if (!userId.equals(otherRelation)) {
                    UserPair pair = new UserPair(userId, otherRelation);
                    context.write(pair, zero);  // Marquer une relation directe pour ne pas la compter
                }
            }
        }
    }
}
