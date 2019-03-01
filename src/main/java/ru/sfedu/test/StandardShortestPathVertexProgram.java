/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package ru.sfedu.test;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.util.NumberHelper;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class StandardShortestPathVertexProgram implements VertexProgram<Triplet<Path, Edge, Number>> {

    @SuppressWarnings("WeakerAccess")
    public static final String SHORTEST_PATHS = "ru.sfedu.test.standardShortestPathVertexProgram.shortestPaths";

    private static final String SOURCE_VERTEX_FILTER = "ru.sfedu.test.standardShortestPathVertexProgram.sourceVertexFilter";
    private static final String TARGET_VERTEX_FILTER = "ru.sfedu.test.standardShortestPathVertexProgram.targetVertexFilter";
    private static final String EDGE_TRAVERSAL = "ru.sfedu.test.standardShortestPathVertexProgram.edgeTraversal";
    private static final String DISTANCE_TRAVERSAL = "ru.sfedu.test.standardShortestPathVertexProgram.distanceTraversal";
    private static final String MAX_DISTANCE = "ru.sfedu.test.standardShortestPathVertexProgram.maxDistance";
    private static final String INCLUDE_EDGES = "ru.sfedu.test.standardShortestPathVertexProgram.includeEdges";

    private static final String STATE = "ru.sfedu.test.standardShortestPathVertexProgram.state";
    private static final String PATHS = "ru.sfedu.test.standardShortestPathVertexProgram.paths";
    private static final String VOTE_TO_HALT = "ru.sfedu.test.standardShortestPathVertexProgram.voteToHalt";

    private static final int SEARCH = 0;
    private static final int COLLECT_PATHS = 1;

    public static final PureTraversal<Vertex, ?> DEFAULT_VERTEX_FILTER_TRAVERSAL = new PureTraversal<>(
            __.<Vertex>identity().asAdmin()); // todo: new IdentityTraversal<>()
    public static final PureTraversal<Vertex, Edge> DEFAULT_EDGE_TRAVERSAL = new PureTraversal<>(__.bothE().asAdmin());
    public static final PureTraversal<Edge, Number> DEFAULT_DISTANCE_TRAVERSAL = new PureTraversal<>(
            __.<Edge>start().<Number>constant(1).asAdmin()); // todo: new ConstantTraversal<>(1)

    private PureTraversal<Vertex, ?> sourceVertexFilterTraversal = DEFAULT_VERTEX_FILTER_TRAVERSAL.clone();
    private PureTraversal<Vertex, ?> targetVertexFilterTraversal = DEFAULT_VERTEX_FILTER_TRAVERSAL.clone();
    private PureTraversal<Vertex, Edge> edgeTraversal = DEFAULT_EDGE_TRAVERSAL.clone();
    private PureTraversal<Edge, Number> distanceTraversal = DEFAULT_DISTANCE_TRAVERSAL.clone();
    private Number maxDistance;
    private boolean distanceEqualsNumberOfHops;
    private boolean includeEdges;

    private static final Set<VertexComputeKey> VERTEX_COMPUTE_KEYS = new HashSet<>(Arrays.asList(
            VertexComputeKey.of(PATHS, true)));

    private final Set<MemoryComputeKey> memoryComputeKeys = new HashSet<>(Arrays.asList(
            MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true),
            MemoryComputeKey.of(STATE, Operator.assign, true, true)));

    private StandardShortestPathVertexProgram() {

    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {

        if (configuration.containsKey(SOURCE_VERTEX_FILTER))
            this.sourceVertexFilterTraversal = PureTraversal.loadState(configuration, SOURCE_VERTEX_FILTER, graph);

        if (configuration.containsKey(TARGET_VERTEX_FILTER))
            this.targetVertexFilterTraversal = PureTraversal.loadState(configuration, TARGET_VERTEX_FILTER, graph);

        if (configuration.containsKey(EDGE_TRAVERSAL))
            this.edgeTraversal = PureTraversal.loadState(configuration, EDGE_TRAVERSAL, graph);

        if (configuration.containsKey(DISTANCE_TRAVERSAL))
            this.distanceTraversal = PureTraversal.loadState(configuration, DISTANCE_TRAVERSAL, graph);

        if (configuration.containsKey(MAX_DISTANCE))
            this.maxDistance = (Number) configuration.getProperty(MAX_DISTANCE);

        this.distanceEqualsNumberOfHops = this.distanceTraversal.equals(DEFAULT_DISTANCE_TRAVERSAL);
        this.includeEdges = configuration.getBoolean(INCLUDE_EDGES, false);

        this.memoryComputeKeys.add(MemoryComputeKey.of(SHORTEST_PATHS, Operator.addAll, true, false));
    }

    @Override
    public void storeState(final Configuration configuration) {
        VertexProgram.super.storeState(configuration);
        this.sourceVertexFilterTraversal.storeState(configuration, SOURCE_VERTEX_FILTER);
        this.targetVertexFilterTraversal.storeState(configuration, TARGET_VERTEX_FILTER);
        this.edgeTraversal.storeState(configuration, EDGE_TRAVERSAL);
        this.distanceTraversal.storeState(configuration, DISTANCE_TRAVERSAL);
        configuration.setProperty(INCLUDE_EDGES, this.includeEdges);
        if (this.maxDistance != null)
            configuration.setProperty(MAX_DISTANCE, maxDistance);
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return VERTEX_COMPUTE_KEYS;
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return memoryComputeKeys;
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return Collections.emptySet();
    }

    @Override
    public VertexProgram<Triplet<Path, Edge, Number>> clone() {
        try {
            final StandardShortestPathVertexProgram clone = (StandardShortestPathVertexProgram) super.clone();
            if (null != this.edgeTraversal)
                clone.edgeTraversal = this.edgeTraversal.clone();
            if (null != this.sourceVertexFilterTraversal)
                clone.sourceVertexFilterTraversal = this.sourceVertexFilterTraversal.clone();
            if (null != this.targetVertexFilterTraversal)
                clone.targetVertexFilterTraversal = this.targetVertexFilterTraversal.clone();
            if (null != this.distanceTraversal)
                clone.distanceTraversal = this.distanceTraversal.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.ORIGINAL;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.NOTHING;
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(VOTE_TO_HALT, true);
        memory.set(STATE, SEARCH);
    }

    @Override
    public void execute(final Vertex vertex, final Messenger<Triplet<Path, Edge, Number>> messenger, final Memory memory) {

        if (memory.<Integer>get(STATE) == COLLECT_PATHS) {
            collectShortestPaths(vertex, memory);
            return;
        }

        boolean voteToHalt = true;

        if (memory.isInitialIteration()) {

            // ignore vertices that don't pass the start-vertex filter
            if (!isStartVertex(vertex)) return;

            // start to track paths for all valid start-vertices
            final Map<Vertex, Pair<Number, Set<Path>>> paths = new HashMap<>();
            final Path path;
            final Set<Path> pathSet = new HashSet<>();

            pathSet.add(path = makePath(vertex));
            paths.put(vertex, Pair.with(0, pathSet));

            vertex.property(VertexProperty.Cardinality.single, PATHS, paths);

            // send messages to valid adjacent vertices
            processEdges(vertex, path, 0, messenger);

            voteToHalt = false;

        } else {

            final Iterator<Triplet<Path, Edge, Number>> iterator = messenger.receiveMessages();

            while (iterator.hasNext()) {
                final Map<Vertex, Pair<Number, Set<Path>>> paths =
                        vertex.<Map<Vertex, Pair<Number, Set<Path>>>>property(PATHS).orElseGet(HashMap::new);

                final Triplet<Path, Edge, Number> triplet = iterator.next();
                final Path sourcePath = triplet.getValue0();
                final Number distance = triplet.getValue2();
                final Vertex sourceVertex = sourcePath.get(0);

                Path newPath = null;

                // already know a path coming from this source vertex?
                if (paths.containsKey(sourceVertex)) {

                    final Number currentShortestDistance = paths.get(sourceVertex).getValue0();
                    final int cmp = NumberHelper.compare(distance, currentShortestDistance);

                    if (cmp <= 0) {
                        newPath = extendPath(sourcePath, triplet.getValue1(), vertex);
                        if (cmp < 0) {
                            // if the path length is smaller than the current shortest path's length, replace the
                            // current set of shortest paths
                            final Set<Path> pathSet = new HashSet<>();
                            pathSet.add(newPath);
                            paths.put(sourceVertex, Pair.with(distance, pathSet));
                        } else {
                            // if the path length is equal to the current shortest path's length, add the new path
                            // to the set of shortest paths
                            paths.get(sourceVertex).getValue1().add(newPath);
                        }
                    }
                } else if (!exceedsMaxDistance(distance)) {
                    // store the new path as the shortest path from the source vertex to the current vertex
                    final Set<Path> pathSet = new HashSet<>();
                    pathSet.add(newPath = extendPath(sourcePath, triplet.getValue1(), vertex));
                    paths.put(sourceVertex, Pair.with(distance, pathSet));
                }

                // if a new path was found, send messages to adjacent vertices, otherwise do nothing as there's no
                // chance to find any new paths going forward
                if (newPath != null) {
                    vertex.property(VertexProperty.Cardinality.single, PATHS, paths);
                    processEdges(vertex, newPath, distance, messenger);
                    voteToHalt = false;
                }
            }
        }

        // VOTE_TO_HALT will be set to true if an iteration hasn't found any new paths
        memory.add(VOTE_TO_HALT, voteToHalt);
    }

    @Override
    public boolean terminate(final Memory memory) {
        final boolean voteToHalt = memory.get(VOTE_TO_HALT);
        if (voteToHalt) {
            final int state = memory.get(STATE);
            if (state == COLLECT_PATHS) {
                return true;
            }
            memory.set(STATE, COLLECT_PATHS); // collect paths if no new paths were found
            return false;
        } else {
            memory.set(VOTE_TO_HALT, true);
            return false;
        }
    }

    @Override
    public String toString() {

        final List<String> options = new ArrayList<>();
        final Function<String, String> shortName = name -> name.substring(name.lastIndexOf(".") + 1);

        if (!this.sourceVertexFilterTraversal.equals(DEFAULT_VERTEX_FILTER_TRAVERSAL)) {
            options.add(shortName.apply(SOURCE_VERTEX_FILTER) + "=" + this.sourceVertexFilterTraversal.get());
        }

        if (!this.targetVertexFilterTraversal.equals(DEFAULT_VERTEX_FILTER_TRAVERSAL)) {
            options.add(shortName.apply(TARGET_VERTEX_FILTER) + "=" + this.targetVertexFilterTraversal.get());
        }

        if (!this.edgeTraversal.equals(DEFAULT_EDGE_TRAVERSAL)) {
            options.add(shortName.apply(EDGE_TRAVERSAL) + "=" + this.edgeTraversal.get());
        }

        if (!this.distanceTraversal.equals(DEFAULT_DISTANCE_TRAVERSAL)) {
            options.add(shortName.apply(DISTANCE_TRAVERSAL) + "=" + this.distanceTraversal.get());
        }

        options.add(shortName.apply(INCLUDE_EDGES) + "=" + this.includeEdges);

        return StringFactory.vertexProgramString(this, String.join(", ", options));
    }

    //////////////////////////////

    private static Path makePath(final Vertex newVertex) {
        return extendPath(null, newVertex);
    }

    private static Path extendPath(final Path currentPath, final Element... elements) {
        Path result = ImmutablePath.make();
        if (currentPath != null) {
            for (final Object o : currentPath.objects()) {
                result = result.extend(o, Collections.emptySet());
            }
        }
        for (final Element element : elements) {
            if (element != null) {
                result = result.extend(ReferenceFactory.detach(element), Collections.emptySet());
            }
        }
        return result;
    }

    private boolean isStartVertex(final Vertex vertex) {
        final Traversal.Admin<Vertex, ?> filterTraversal = this.sourceVertexFilterTraversal.getPure();
        filterTraversal.addStart(filterTraversal.getTraverserGenerator().generate(vertex, filterTraversal.getStartStep(), 1));
        return filterTraversal.hasNext();
    }

    private boolean isEndVertex(final Vertex vertex) {
        final Traversal.Admin<Vertex, ?> filterTraversal = this.targetVertexFilterTraversal.getPure();
        //noinspection unchecked
        final Step<Vertex, Vertex> startStep = (Step<Vertex, Vertex>) filterTraversal.getStartStep();
        filterTraversal.addStart(filterTraversal.getTraverserGenerator().generate(vertex, startStep, 1));
        return filterTraversal.hasNext();
    }

    private void processEdges(final Vertex vertex, final Path currentPath, final Number currentDistance,
                              final Messenger<Triplet<Path, Edge, Number>> messenger) {

        final Traversal.Admin<Vertex, Edge> edgeTraversal = this.edgeTraversal.getPure();
        edgeTraversal.addStart(edgeTraversal.getTraverserGenerator().generate(vertex, edgeTraversal.getStartStep(), 1));

        while (edgeTraversal.hasNext()) {
            final Edge edge = edgeTraversal.next();
            final Number distance = getDistance(edge);

            Vertex otherV = edge.inVertex();
            if (otherV.equals(vertex))
                otherV = edge.outVertex();

            // only send message if the adjacent vertex is not yet part of the current path
            if (!currentPath.objects().contains(otherV)) {
                messenger.sendMessage(MessageScope.Global.of(otherV),
                        Triplet.with(currentPath, this.includeEdges ? edge : null,
                                NumberHelper.add(currentDistance, distance)));
            }
        }
    }

    private Number getDistance(final Edge edge) {
        if (this.distanceEqualsNumberOfHops) return 1;
        final Traversal.Admin<Edge, Number> traversal = this.distanceTraversal.getPure();
        traversal.addStart(traversal.getTraverserGenerator().generate(edge, traversal.getStartStep(), 1));
        return traversal.tryNext().orElse(0);
    }

    private boolean exceedsMaxDistance(final Number distance) {
        // This method is used to stop the message sending for paths that exceed the specified maximum distance. Since
        // custom distances can be negative, this method should only return true if the distance is calculated based on
        // the number of hops.
        return this.distanceEqualsNumberOfHops && this.maxDistance != null
                && NumberHelper.compare(distance, this.maxDistance) > 0;
    }

    /**
     * Move any valid path into the VP's memory.
     *
     * @param vertex The current vertex.
     * @param memory The VertexProgram's memory.
     */
    private void collectShortestPaths(final Vertex vertex, final Memory memory) {

        final VertexProperty<Map<Vertex, Pair<Number, Set<Path>>>> pathProperty = vertex.property(PATHS);

        if (pathProperty.isPresent()) {
            if (isEndVertex(vertex)) {
                final Map<Vertex, Pair<Number, Set<Path>>> paths = pathProperty.value();
                final List<Path> result = new ArrayList<>();

                for (final Pair<Number, Set<Path>> pair : paths.values()) {
                    for (final Path path : pair.getValue1()) {
                        if (this.distanceEqualsNumberOfHops ||
                                this.maxDistance == null ||
                                NumberHelper.compare(pair.getValue0(), this.maxDistance) <= 0) {
                            result.add(path);
                        }
                    }
                }

                memory.add(SHORTEST_PATHS, result);
            }

            pathProperty.remove();
        }
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    @SuppressWarnings("WeakerAccess")
    public static final class Builder extends AbstractVertexProgramBuilder<Builder> {


        private Builder() {
            super(StandardShortestPathVertexProgram.class);
        }

        public Builder source(final Traversal<Vertex, ?> sourceVertexFilter) {
            if (null == sourceVertexFilter) throw Graph.Exceptions.argumentCanNotBeNull("sourceVertexFilter");
            PureTraversal.storeState(this.configuration, SOURCE_VERTEX_FILTER, sourceVertexFilter.asAdmin());
            return this;
        }

        public Builder target(final Traversal<Vertex, ?> targetVertexFilter) {
            if (null == targetVertexFilter) throw Graph.Exceptions.argumentCanNotBeNull("targetVertexFilter");
            PureTraversal.storeState(this.configuration, TARGET_VERTEX_FILTER, targetVertexFilter.asAdmin());
            return this;
        }

        public Builder edgeDirection(final Direction direction) {
            if (null == direction) throw Graph.Exceptions.argumentCanNotBeNull("direction");
            return edgeTraversal(__.toE(direction));
        }

        public Builder edgeTraversal(final Traversal<Vertex, Edge> edgeTraversal) {
            if (null == edgeTraversal) throw Graph.Exceptions.argumentCanNotBeNull("edgeTraversal");
            PureTraversal.storeState(this.configuration, EDGE_TRAVERSAL, edgeTraversal.asAdmin());
            return this;
        }

        public Builder distanceProperty(final String distance) {
            //noinspection unchecked
            return distance != null
                    ? distanceTraversal(__.values(distance)) // todo: (Traversal) new ElementValueTraversal<>(distance)
                    : distanceTraversal(DEFAULT_DISTANCE_TRAVERSAL.getPure());
        }

        public Builder distanceTraversal(final Traversal<Edge, Number> distanceTraversal) {
            if (null == distanceTraversal) throw Graph.Exceptions.argumentCanNotBeNull("distanceTraversal");
            PureTraversal.storeState(this.configuration, DISTANCE_TRAVERSAL, distanceTraversal.asAdmin());
            return this;
        }

        public Builder maxDistance(final Number distance) {
            if (null != distance)
                this.configuration.setProperty(MAX_DISTANCE, distance);
            else
                this.configuration.clearProperty(MAX_DISTANCE);
            return this;
        }

        public Builder includeEdges(final boolean include) {
            this.configuration.setProperty(INCLUDE_EDGES, include);
            return this;
        }
    }

    ////////////////////////////

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresGlobalMessageScopes() {
                return true;
            }

            @Override
            public boolean requiresVertexPropertyAddition() {
                return true;
            }
        };
    }
}