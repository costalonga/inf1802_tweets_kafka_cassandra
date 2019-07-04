package com.example.crudtwitter;

import com.datastax.driver.core.Session;

// Repository to handle the Cassandra schema
public class KeyspaceRepository {
    private Session session;

    public KeyspaceRepository(Session session) {
        this.session = session;
    }

    public void createKeyspace(String keyspaceName, String replicatioonStrategy, int numberOfReplicas) {
        System.out.println("createKeyspace - init");

        StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ").append(keyspaceName)
                .append(" WITH replication = {").append("'class':'")
                .append(replicatioonStrategy)
                .append("','replication_factor':")
                .append(numberOfReplicas).append("};");

        final String query = sb.toString();
        session.execute(query);

        System.out.println("createKeyspace - end");
    }

    public void useKeyspace(String keyspace) {
        System.out.println("useKeyspace - init");
        session.execute("USE " + keyspace);
        System.out.println("useKeyspace - end");
    }

    public void deleteKeyspace(String keyspaceName) {
        System.out.println("deleteKeyspace - init");
        StringBuilder sb = new StringBuilder("DROP KEYSPACE ").append(keyspaceName);

        final String query = sb.toString();
        session.execute(query);
        System.out.println("deleteKeyspace - end");
    }

}
