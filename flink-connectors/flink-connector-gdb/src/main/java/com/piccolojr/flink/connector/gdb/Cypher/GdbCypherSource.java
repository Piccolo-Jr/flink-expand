package com.piccolojr.flink.connector.gdb.Cypher;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.neo4j.driver.*;

import java.io.IOException;
import java.util.List;

@Experimental
public class GdbCypherSource extends RichSourceFunction<List<Record>> {

    protected Driver driver;
    protected final String uri;
    protected final String user;
    protected final String password;
    protected final String cypherQuery;
    protected transient volatile boolean isRunning;

    public GdbCypherSource(String uri, String user, String password, String cypherQuery) {
        this.uri = uri;
        this.user = user;
        this.password = password;
        this.cypherQuery = cypherQuery;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
        isRunning = true;
    }

    @Override
    public void run(SourceContext<List<Record>> sourceContext) throws Exception {

        try ( Session session = driver.session() )
        {
            List<Record> records = session.writeTransaction(new TransactionWork<List<Record>>()
            {
                @Override
                public List<Record> execute(Transaction tx )
                {
                    Result result = tx.run(cypherQuery);
                    return result.list();
                }
            });
            sourceContext.collect(records);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        super.close();
        driver.close();
        isRunning = false;
    }

    public static GdbCypherSourceBuilder newBuilder() {
        return new GdbCypherSourceBuilder();
    }

    public static class GdbCypherSourceBuilder {
        private String uri;
        private String user;
        private String password;
        private String cypherQuery;

        public GdbCypherSourceBuilder withUri(String uri) {
            this.uri = uri;
            return this;
        }

        public GdbCypherSourceBuilder withUser(String user) {
            this.user = user;
            return this;
        }

        public GdbCypherSourceBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        public GdbCypherSourceBuilder withCypherQuery(String cypherQuery) {
            this.cypherQuery = cypherQuery;
            return this;
        }

        public GdbCypherSource build() throws IOException {
            return new GdbCypherSource(uri,user,password,cypherQuery);
        }
    }
}
