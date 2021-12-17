package org.twilmes.sql.gremlin.adapter.converter.schema;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.calcite.util.Pair;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinEdgeTable;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinProperty;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinVertexTable;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class HugeGraphSchemaGrabber {
    private static final Logger LOGGER = LoggerFactory.getLogger(HugeGraphSchemaGrabber.class);
    private static final Map<Class<?>, String> TYPE_MAP = new HashMap<>();
    private static final String VERTEX_LABELS_QUERY = "g.getGraph().schema().getVertexLabels().collect {[it.name(), it.properties().collect{it.asLong()}]}";
    private static final String EDGE_LABELS_QUERY = "g.getGraph().schema().getEdgeLabels().collect {[it.name(), it.properties().collect{it.asLong()}, it.sourceLabelName(), it.targetLabelName()]}";
    private static final String PROPERTY_KEYS_QUERY = " g.getGraph().schema().getPropertyKeys().collect{[it.id().asLong(), it.name(),  it.dataType().clazz().getSimpleName()]}";

    private HugeGraphSchemaGrabber() {
    }

    public static GremlinSchema getSchema(final Client client) throws SQLException {

        try {
            // send queries
            CompletableFuture<ResultSet> propertyFuture = client.submitAsync(PROPERTY_KEYS_QUERY);
            CompletableFuture<ResultSet> edgeFuture = client.submitAsync(EDGE_LABELS_QUERY);
            CompletableFuture<ResultSet> vertexFuture = client.submitAsync(VERTEX_LABELS_QUERY);

            // collect properties/columns
            Stream<List<Object>> propertyStream = propertyFuture.get().stream().map(entry -> entry.get(List.class));
            Map<Long, GremlinProperty> properties = propertyStream.collect(Collectors.toMap(
                    entry -> (Long)entry.get(0),
                    entry -> new GremlinProperty((String)entry.get(1), fixType((String)entry.get(2)))
            ));

            // collect edges
            //Assumes single  pair of in,out vertices per edge (that is true in HugeGraph)
            List<GremlinEdgeTable> edgeList = edgeFuture.get().stream().map(
                    entry -> {
                        List entryList = entry.get(List.class);
                        String name = (String)entryList.get(0);
                        List<Long> propertyIds = (List<Long>) entryList.get(1);
                        List<GremlinProperty> edgeProperties = propertyIds.stream().map(properties::get).collect(Collectors.toList());
                        return new GremlinEdgeTable(name, edgeProperties ,
                                Arrays.asList(new Pair(entryList.get(3), entryList.get(2))));
                    }).collect(Collectors.toList());


            // collect in/out edges for vertex schema
            Map<String, List<GremlinEdgeTable>> inEdgesMap = edgeList.stream().collect(Collectors.groupingBy(
                    edge ->  edge.getInOutVertexPairs().get(0).getKey() ,Collectors.toList()));
            Map<String, List<GremlinEdgeTable>> outEdgesMap = edgeList.stream().collect(Collectors.groupingBy(
                    edge ->  edge.getInOutVertexPairs().get(0).getValue() ,Collectors.toList()));

            //collect vertices
            List<GremlinVertexTable> vertexList = vertexFuture.get().stream().map(
                    entry -> {
                        List<Object> entryList = entry.get(List.class);
                        String name = (String)entryList.get(0);
                        List<Long> propertyIds = (List<Long>) entryList.get(1);
                        List<GremlinProperty> vertexProperties = propertyIds.stream().map(properties::get)
                                .collect(Collectors.toList());
                        List<String> inEdges = collectEdgeNames(inEdgesMap.getOrDefault(name, Collections.EMPTY_LIST));
                        List<String> outEdges = collectEdgeNames(outEdgesMap.getOrDefault(name, Collections.EMPTY_LIST));
                        return new GremlinVertexTable(name, vertexProperties ,inEdges, outEdges);
                    }).collect(Collectors.toList());

            return new GremlinSchema(vertexList, edgeList);
        } catch (final ExecutionException | InterruptedException e) {
            LOGGER.error("fail to query schema", e);
            throw new SQLException("Error occurred during schema collection. '" + e.getMessage() + "'.");
        }
    }

    private static String fixType(String type) {
        return type.equals("Date")?"String":type;
    }

    @NotNull
    private static List<String> collectEdgeNames(List<GremlinEdgeTable> inEdges) {
        return inEdges.stream()
                .map(e -> e.getLabel()).collect(Collectors.toList());
    }
}
