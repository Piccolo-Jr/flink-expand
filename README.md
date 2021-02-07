# flink-expand

Some extension development programs about flink for self-learning and communication.

## samples
* Read data from GDB use Gremlin

```StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironment();
 streamEnv.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

 GdbGremlinDeserializationSchema<Edge<String, NullValue>> deserializationSchema = new GdbGremlinDeserializationSchema<Edge<String,NullValue>>() {
     @Override
     public TypeInformation<Edge<String,NullValue>> getProducedType() {
         return TypeInformation.of(new TypeHint<Edge<String, NullValue>>(){});
     }
     @Override
     public Edge<String,NullValue> deserialize(Tuple message) throws Exception {
         return (Edge<String, NullValue>) message;
     }
 };

 GdbGremlinSource<Edge<String,NullValue>> gdbGremlinSource = GdbGremlinSource.<Edge<String,NullValue>>newBuilder()
     .withGdbGremlinDeserializationSchema(deserializationSchema)
     .withContactPoint(testContactPoint)
     .withPort(testPort)
     .withUsername(testUsername)
     .withPassword(testPassword)
     .withGremlinQuery("g.E()")
     .build();

 streamEnv.addSource(gdbGremlinSource).print();

 streamEnv.execute();
 ```
