package com.piccolojr.flink.connector.gdb.Gremlin;

import com.piccolojr.flink.connector.gdb.Gremlin.common.GdbGremlinConnectorBuilder;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;

import java.util.HashMap;
import java.util.Map;

@Experimental
public class GdbGremlinSink<IN> extends RichSinkFunction<IN> {

    protected Cluster cluster;
    protected Client client;
    protected final String contactPoint;
    protected final Integer port;
    protected final String username;
    protected final String password;
    protected final boolean enableSsl;
    protected final String trustStore;
    protected transient volatile boolean isRunning;

    GdbGremlinSink(String contactPoint,Integer port,String username, String password, boolean enableSsl, String trustStore){
        this.contactPoint = contactPoint;
        this.port = port;
        this.username = username;
        this.password = password;
        this.enableSsl = enableSsl;
        this.trustStore = trustStore;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        if (enableSsl){
            if (!trustStore.equals("")){
                cluster = Cluster.build().credentials(username, password).enableSsl(true).trustStore(trustStore).create();
            } else {
                throw new Exception("Please give a valid truststore.");
            }
        }else {
            cluster = Cluster.build().credentials(username, password).create();
        }
        client = cluster.connect().init();
        this.isRunning = true;
    }

    @Override
    public void close() throws Exception {
        super.close();
        client.close();
        cluster.close();
        this.isRunning = false;
    }

    @Override
    public void invoke(IN message, Context context) throws Exception{
        Map<String,Object> parameters = new HashMap<>();
        String dsl = "";
        if (message.getClass().equals(Vertex.class)){
            dsl += "g.addV(label)";
            parameters.put("label",((Vertex<?, ?>) message).getId());
            client.submit(dsl,parameters);
        } else if(message.getClass().equals(Edge.class)) {
            dsl += "g.addE().from(V(sourceId)).to(V(targetId))";
            parameters.put("sourceId",((Edge<?, ?>) message).getSource());
            parameters.put("targetId",((Edge<?, ?>) message).getTarget());
            client.submit(dsl,parameters);
        } else {
            throw new Exception("Not valid graph type.");
        }
    }

    public static <IN extends Tuple> GdbGremlinSinkBuilder<IN> newBuilder() {
        return new GdbGremlinSinkBuilder<>();
    }

    public static class GdbGremlinSinkBuilder<IN extends Tuple> extends GdbGremlinConnectorBuilder<GdbGremlinSinkBuilder<IN>> {

        @Override
        public GdbGremlinSinkBuilder<IN> getThis() {
            return this;
        }

        public GdbGremlinSink<IN> build() {
            validate();
            return new GdbGremlinSink<>(contactPoint,port,username,password,enableSsl,trustStore);
        }
    }
}
