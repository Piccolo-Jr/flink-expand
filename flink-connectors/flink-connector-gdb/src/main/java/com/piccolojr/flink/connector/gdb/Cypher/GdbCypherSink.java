package com.piccolojr.flink.connector.gdb.Cypher;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.neo4j.driver.*;

import java.io.IOException;

@Experimental
public class GdbCypherSink extends RichSinkFunction<Record> {

    protected Driver driver;
    protected final String uri;
    protected final String user;
    protected final String password;
    protected final String cypherQuery;
    protected transient volatile boolean isRunning;

    public GdbCypherSink(String uri, String user, String password, String cypherQuery) {
        this.uri = uri;
        this.user = user;
        this.password = password;
        this.cypherQuery = cypherQuery;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password) );
        isRunning = true;
    }

    @Override
    public void invoke(Record message, Context context) {

        try ( Session session = driver.session() )
        {
            session.writeTransaction(new TransactionWork<String>()
            {
                @Override
                public String execute(Transaction tx )
                {
                    Result result = tx.run(cypherQuery);
                   return "";
                }
            } );
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        driver.close();
        isRunning = false;
    }

    public static GdbCypherSinkBuilder newBuilder() {
        return new GdbCypherSinkBuilder();
    }

    public static class GdbCypherSinkBuilder {
        private String uri;
        private String user;
        private String password;
        private String cypherQuery;

        public GdbCypherSinkBuilder withUri(String uri) {
            this.uri = uri;
            return this;
        }

        public GdbCypherSinkBuilder withUser(String user) {
            this.user = user;
            return this;
        }

        public GdbCypherSinkBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        public GdbCypherSinkBuilder withCypherQuery(String cypherQuery) {
            this.cypherQuery = cypherQuery;
            return this;
        }

        public GdbCypherSink build() throws IOException {
            return new GdbCypherSink(uri,user,password,cypherQuery);
        }
    }
}
