package com.paradox.geek.topology;

import com.paradox.geek.bolt.HashtagCounterBolt;
import com.paradox.geek.bolt.HashtagReaderBolt;
import com.paradox.geek.spout.TwitterSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;

public class TwitterHashtagTopology{
    public static void main(String[] args) throws Exception {
        String consumerKey = args[0];
        String consumerSecret = args[1];

        String accessToken = args[2];
        String accessTokenSecret = args[3];

        String[] arguments = args.clone();
        String[] keyWords = Arrays.copyOfRange(arguments, 4, arguments.length);

        Config config = new Config();
        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout", new TwitterSpout(consumerKey,
                consumerSecret, accessToken, accessTokenSecret, keyWords));

        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
                .shuffleGrouping("twitter-spout");

        builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt())
                .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TwitterHashtag", config, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
