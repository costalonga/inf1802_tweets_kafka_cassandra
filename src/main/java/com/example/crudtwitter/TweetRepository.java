package com.example.crudtwitter;

import java.util.ArrayList;
import java.util.List;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import twitter4j.*;

public class TweetRepository {

    private static final String TABLE_NAME = "tweets";
    private static final String TABLE_NAME_BY_LANG   = TABLE_NAME + "ByLang";
    private Session session;

    public TweetRepository(Session session) {this.session = session;}

    // Criar tabela tweets
    public void createTable() {
        System.out.println("\tcreateTable - init");
        // TODO: Verificar tipos de dados que vão ser inseridos no cassandra (usar tudo como string 'text'?)
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME).append("(")
                .append("id bigint PRIMARY KEY, ")
                .append("CreatedDate text,")
                .append("Content text,")
                .append("Source text,")
                .append("IsTruncated text,")
                .append("Latitude text,")
                .append("Longitude text,")
                .append("IsFavorited text,")
                .append("UserName text,")
                .append("Contributors text,")
                .append("Language text);");

//        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
//                .append(TABLE_NAME).append("(")
//                .append("id long PRIMARY KEY, ")
//                .append("content text,")
//                .append("latitude text,")
//                .append("longitude text,")
//                .append("userName text,")
//                .append("language text);");

        System.out.println("\tcreateTable - command: " + sb);
        System.out.println("\tcreateTable - end\n");
        final String query = sb.toString();
        session.execute(query);
    }

    // Criar tabela tweetsByLang
    /*
        Justificativa: Lang (Language) é um atributo interessante para ser uma PartitionKey,
       pois será possível dividir vários tweets por suas língua, com isso será possível analisar
       quais línguas publicam mais tweets, entre outras buscas... (visto também que o número de línguas
       é bem menor que o número de tweets)
    */
    public void createTableTweetsByLang() {
        System.out.println("\tcreateTableTweetsByLang - init");
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME_BY_LANG).append("(")
                .append("id bigint, ")
                .append("content text,")
                .append("userName text,")
                .append("language text,")
                .append("PRIMARY KEY (id, language));");
        // TODO add Latitude/Longitude and CreatedAt

        System.out.println("\tcreateTableTweetsByLang - command: " + sb);
        System.out.println("\tcreateTableTweetsByLang - end\n");
        final String query = sb.toString();
        session.execute(query);
    }

    // Inserir uma tupla na tabela tweets
    // @param tweet
    public void insertTweet(Tweet tweet) {
        System.out.println("\tinsertTweet - init");
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME).append("(id, content, latitude, longitude, userName, language) ")
                .append("VALUES (").append(tweet.getId()).append(", '")
                .append(tweet.getText()).append("', '")
                .append(tweet.getLatitude()).append("', '")
                .append(tweet.getLongitude()).append("', '")
                .append(tweet.getUserName()).append("', '")
                .append(tweet.getLanguage()).append("');");
        // TODO add Latitude/Longitude and CreatedAt

        System.out.println("\tinsertTweet - command: " + sb);
        System.out.println("\tinsertTweet - end\n");
        final String query = sb.toString();
        session.execute(query);
    }

    // Inserir uma tupla na tabela tweetsByLang
    // @param tweet
    public void insertTweetsByLang(Tweet tweet) {
        System.out.println("\tinsertTweetsByLang - init");
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME_BY_LANG).append("(id, content, userName, language) ")
                .append("VALUES (").append(tweet.getId()).append(", '")
                .append(tweet.getText()).append("', '")
                .append(tweet.getUserName()).append("', '")
                .append(tweet.getLanguage()).append("');");

        System.out.println("\tinsertTweetsByLang - command: " + sb);
        System.out.println("\tinsertTweetsByLang - end\n");
        final String query = sb.toString();
        session.execute(query);
    }

    // Apresentar todas as tuplas da tabela tweets
    // @return
    public List<Tweet> selectAll() {
        System.out.println("\tselectAll - init");
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(TABLE_NAME);
        System.out.println("\tselectAll - command: " + sb);

        final String query = sb.toString();
        ResultSet rs = session.execute(query);

        List<Tweet> tweets = new ArrayList<Tweet>();

        for (Row r : rs) {
            Tweet tw = new Tweet(
                    r.getLong("id"),
                    r.getString("content"),
                    r.getString("latitude"),
                    r.getString("longitude"),
                    r.getString("userName"),
                    r.getString("language"));

            System.out.println("\t\tId = " + r.getLong("id") + ", "
                    + "Message = " + r.getString("content") + ", "
                    + "Latitude = " + r.getString("latitude") + ", "
                    + "Longitude = " + r.getString("longitude") + ", "
                    + "User = " + r.getString("userName") + ", "
                    + "Language = " + r.getString("language"));
            tweets.add(tw);
        }
        System.out.println("\tselectAll - end\n");
        return tweets;
    }

    // Apresentar todas as tuplas da tabela tweets
    // @return
    public List<Tweet> selectAllTweetsByLang() {
        System.out.println("\tselectAllTweetsByLang - init");
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(TABLE_NAME_BY_LANG);
        System.out.println("\tselectAllTweetsByLang - command: " + sb);

        final String query = sb.toString();
        ResultSet rs = session.execute(query);

        List<Tweet> tweets = new ArrayList<Tweet>();

        for (Row r : rs) {
            Tweet tw = new Tweet(
                    r.getLong("id"),
                    r.getString("content"),
                   null,
                    null,
                    r.getString("userName"),
                    r.getString("language"));

            System.out.println("\t\tId = " + r.getLong("id") + ", "
                    + "Message = " + r.getString("content") + ", "
                    + "User = " + r.getString("userName") + ", "
                    + "Language = " + r.getString("language"));
            tweets.add(tw);
        }
        System.out.println("\tselectAllTweetsByLang - end\n");
        return tweets;
    }

    // Apagar uma determinada tupla
    public void deleteTweet(Long id) {
        System.out.println("\tdeleteTweet - init");
        StringBuilder sb = new StringBuilder("DELETE FROM ")
                .append(TABLE_NAME)
                .append(" WHERE id = ")
                .append(id).append(";");

        System.out.println("\tdeleteTweet - command: " + sb);
        System.out.println("\tdeleteTweet - end\n");
        final String query = sb.toString();
        session.execute(query);
    }

    // Drop tabela tweets
    //@param tableName the name of the table to delete
    public void deleteTable(String tableName) {
        System.out.println("\tdeleteTable - init");
        StringBuilder sb = new StringBuilder("DROP TABLE IF EXISTS ").append(tableName);

        System.out.println("\tdeleteTable - command: " + sb);
        System.out.println("\tdeleteTable - end\n");
        final String query = sb.toString();
        session.execute(query);
    }
}











