package com.example.crudtwitter;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.util.ArrayList;

public class HelloTweet {

    public static void main(String[] args) {
        System.out.println("HelloTweet!");

        Cluster cluster = null;

        try {
            cluster = Cluster.builder().addContactPoint("localhost").build();

            Session session = cluster.connect();

            ResultSet rs = session.execute("Select release_version from system.local");
            Row row = rs.one();
            System.out.println(row.getString("release_version"));

            KeyspaceRepository sr = new KeyspaceRepository(session);

            //Cria keyspace
            sr.createKeyspace("library", "SimpleStrategy", 1);
            System.out.println("Create repository");

            sr.useKeyspace("library");
            System.out.println("Using repository library");

            // Cria tabela tweets
            TweetRepository tweets = new TweetRepository(session);
            tweets.createTable();
            System.out.println("Create table tweets");

            // Cria tabela tweetsByLang
            tweets.createTableTweetsByLang();
            System.out.println("Create table tweetsByLang - step 2");


            // TODO: Change to use twitter4j api to get real tweets
            ArrayList<String> languages = new ArrayList<String>();
            languages.add("BR");
            languages.add("US");
            languages.add("ES");

            for (int i = 0, j = 0; i < 5; i++, j++) {
                if (j == languages.size()) { j = 0; }
                String lat = "lat"+i;
                String lon = "lon"+i;
                String lang = languages.get(j);

                Tweet tw = new Tweet(new Long(i), "Message_"+i, lat, lon, "User_"+i, lang);
                tweets.insertTweet(tw);
                tweets.insertTweetsByLang(tw);
                System.out.println("Inserted tweet in tables");
            }

            tweets.selectAll(); // Apresenta todas as tuplas
            tweets.selectAllTweetsByLang(); // Apresenta todas as tuplas
            tweets.deleteTweet(new Long(2)); // Deleta um tweet da tabela
            tweets.selectAll();
            tweets.selectAllTweetsByLang(); // Apresenta todas as tuplas

            tweets.deleteTable("tweets");
            System.out.println("Deleted tweets");

            sr.deleteKeyspace("library");
            System.out.println("Delete keyspace library");

        } finally {
            if (cluster != null) cluster.close();
        }

    }

}

