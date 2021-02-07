package com.piccolojr.flink.connector.gdb.Gremlin;

import com.piccolojr.flink.connector.gdb.Gremlin.common.GdbGremlinConnectorBuilder;
import com.piccolojr.flink.connector.gdb.Gremlin.common.GdbGremlinDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferencePath;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;

import java.util.*;

public class GdbGremlinSource<OUT extends Tuple> extends RichSourceFunction<OUT> implements ResultTypeQueryable<OUT> {

    protected Cluster cluster;
    protected Client client;
    protected GdbGremlinDeserializationSchema<OUT> deserializationSchema;
    protected final String contactPoint;
    protected final int port;
    protected final String username;
    protected final String password;
    protected final boolean enableSsl;
    protected final String trustStore;
    protected final String gremlinQuery;
    protected transient volatile boolean isRunning;

    GdbGremlinSource(GdbGremlinDeserializationSchema<OUT> deserializationSchema,
                     String contactPoint,
                     int port,
                     String username,
                     String password,
                     boolean enableSsl,
                     String trustStore,
                     String gremlinQuery){
        this.deserializationSchema = deserializationSchema;
        this.contactPoint = contactPoint;
        this.port = port;
        this.username = username;
        this.password = password;
        this.enableSsl = enableSsl;
        this.trustStore = trustStore;
        this.gremlinQuery = gremlinQuery;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        if (enableSsl){
            if (!trustStore.equals("")){
                cluster = Cluster.build()
                        .addContactPoint(contactPoint)
                        .port(port)
                        .credentials(username, password)
                        .enableSsl(true).trustStore(trustStore)
                        .serializer(Serializers.GRAPHBINARY_V1D0)
                        .create();
            } else {
                throw new Exception("Not a valid truststore.");
            }
        }else {
            cluster = Cluster.build()
                    .addContactPoint(contactPoint)
                    .port(port)
                    .credentials(username, password)
                    .serializer(Serializers.GRAPHBINARY_V1D0)
                    .create();
        }
        client = cluster.connect().init();
        this.isRunning = true;
    }

    @Override
    public void run(SourceContext<OUT> sourceContext) throws Exception {

        GdbGremlinCollector collector = new GdbGremlinCollector(sourceContext);

        processResult(client.submit(gremlinQuery).all().join(), collector);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        super.close();
        client.close();
        cluster.close();
        this.isRunning = false;
    }

    private void processResult(List<Result> results, GdbGremlinCollector collector) throws Exception{

        for (Result result : results){
            if (result.getObject().getClass().equals(ReferenceVertex.class)){
                deserializationSchema.deserialize(
                        new Vertex<>((String) result.getVertex().id(), NullValue.getInstance()),
                        collector);
            } else if (result.getObject().getClass().equals(ReferenceEdge.class)) {
                deserializationSchema.deserialize(
                        new Edge<>((String) result.getEdge().inVertex().id(), (String) result.getEdge().outVertex().id(), NullValue.getInstance()),
                        collector);
            } else if (result.getObject().getClass().equals(ReferencePath.class)){
                result.getPath().stream().iterator().forEachRemaining(
                        pair -> {
                            try {
                                deserializationSchema.deserialize(
                                        new Vertex<>(((ReferenceVertex)pair.getValue0()).id(),NullValue.getInstance()),
                                        collector);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                );
            } else if (result.getObject().getClass().equals(ReferenceVertexProperty.class)){
                deserializationSchema.deserialize(
                        new Tuple2<>(result.getVertexProperty().key(),result.getVertexProperty().value()),
                        collector);
            } else if (result.getObject().getClass().equals(ArrayList.class)){
                for (Object ele :  result.get(ArrayList.class)) {
                    if (ele.getClass().equals(ReferenceVertex.class)) {
                        deserializationSchema.deserialize(
                                new Vertex<>((String) ((ReferenceVertex) ele).id(), NullValue.getInstance()),
                                collector);
                    } else if (ele.getClass().equals(ReferenceEdge.class)) {
                        deserializationSchema.deserialize(
                                new Edge<>((String) ((ReferenceEdge) ele).inVertex().id(), (String) ((ReferenceEdge) ele).outVertex().id(), NullValue.getInstance()),
                                collector);
                    } else if (ele.getClass().equals(ReferenceVertexProperty.class)){
                        deserializationSchema.deserialize(
                                new Tuple2<>((((ReferenceVertexProperty<?>) ele)).key(), ((ReferenceVertexProperty<?>) ele).value()),
                                collector);
                    }
                }
            } else {
                throw new Exception("Types not currently supported.");
            }
        }

    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    private class GdbGremlinCollector implements Collector<OUT>{

        private final SourceContext<OUT> ctx;

        private GdbGremlinCollector(SourceContext<OUT> ctx) {
            this.ctx = ctx;
        }

        @Override
        public void collect(OUT out) {
            ctx.collect(out);
        }

        @Override
        public void close() {}
    }

    public static <OUT extends Tuple> GdbGremlinSourceBuilder<OUT> newBuilder() {
        return new GdbGremlinSourceBuilder<>();
    }

    public static class GdbGremlinSourceBuilder<OUT extends Tuple> extends GdbGremlinConnectorBuilder<GdbGremlinSourceBuilder<OUT>> {

        private GdbGremlinDeserializationSchema<OUT> deserializationSchema;
        private String gremlinQuery;

        public GdbGremlinSourceBuilder<OUT> withGdbGremlinDeserializationSchema(
                GdbGremlinDeserializationSchema<OUT> deserializationSchema){
            this.deserializationSchema = deserializationSchema;
            return this;
        }

        public GdbGremlinSourceBuilder<OUT> withGremlinQuery(String gremlinQuery) {
            this.gremlinQuery = gremlinQuery;
            return this;
        }

        @Override
        public GdbGremlinSourceBuilder<OUT> getThis(){
            return this;
        }

        public GdbGremlinSource<OUT> build(){
            validate();
            if (gremlinQuery == null || gremlinQuery.equals("")) {
                throw new IllegalArgumentException("No gremlinQuery was supplied.");
            }
            return new GdbGremlinSource<>(deserializationSchema,contactPoint,port,username,password,enableSsl,trustStore,gremlinQuery);
        }
    }
}
