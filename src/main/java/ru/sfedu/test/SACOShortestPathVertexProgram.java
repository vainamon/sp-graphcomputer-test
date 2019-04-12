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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class SACOShortestPathVertexProgram implements VertexProgram<SACOShortestPathVertexProgram.Ant> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SACOShortestPathVertexProgram.class);

    @SuppressWarnings("WeakerAccess")
    public static final String SHORTEST_PATHS = "ru.sfedu.test.SACOShortestPathVertexProgram.shortestPaths";

    private static final String SOURCE_VERTEX_FILTER = "ru.sfedu.test.SACOShortestPathVertexProgram.sourceVertexFilter";
    private static final String TARGET_VERTEX_FILTER = "ru.sfedu.test.SACOShortestPathVertexProgram.targetVertexFilter";
    private static final String EDGE_TRAVERSAL = "ru.sfedu.test.SACOShortestPathVertexProgram.edgeTraversal";
    private static final String DISTANCE_TRAVERSAL = "ru.sfedu.test.SACOShortestPathVertexProgram.distanceTraversal";
    private static final String MAX_DISTANCE = "ru.sfedu.test.SACOShortestPathVertexProgram.maxDistance";
    private static final String INCLUDE_EDGES = "ru.sfedu.test.SACOShortestPathVertexProgram.includeEdges";
    private static final String ANTS_NUMBER = "ru.sfedu.test.SACOShortestPathVertexProgram.antsNumber";
    private static final String ALPHA = "ru.sfedu.test.SACOShortestPathVertexProgram.alpha";
    private static final String RHO = "ru.sfedu.test.SACOShortestPathVertexProgram.rho";
    private static final String ITERATIONS = "ru.sfedu.test.SACOShortestPathVertexProgram.iterations";

    private static final String EDGE_PHEROMONE = "ru.sfedu.test.SACOShortestPathVertexProgram.pheromone";
    private static final String STATE = "ru.sfedu.test.SACOShortestPathVertexProgram.state";
    private static final String PATHS = "ru.sfedu.test.SACOShortestPathVertexProgram.paths";
    private static final String VOTE_TO_HALT = "ru.sfedu.test.SACOShortestPathVertexProgram.voteToHalt";
    private static final String ITERATION = "ru.sfedu.test.SACOShortestPathVertexProgram.iteration";

    private static final int START = 0;
    private static final int SEARCH = 1;
    private static final int PHEROMONE_EVAPORATION = 2;
    private static final int COLLECT_PATHS = 3;

    public static final PureTraversal<Vertex, ?> DEFAULT_VERTEX_FILTER_TRAVERSAL = new PureTraversal<>(
            __.<Vertex>identity().asAdmin()); // todo: new IdentityTraversal<>()
    public static final PureTraversal<Vertex, Edge> DEFAULT_EDGE_TRAVERSAL = new PureTraversal<>(__.bothE().asAdmin());
    public static final PureTraversal<Edge, Number> DEFAULT_DISTANCE_TRAVERSAL = new PureTraversal<>(
            __.<Edge>start().<Number>constant(1).asAdmin()); // todo: new ConstantTraversal<>(1)

    public static final PureTraversal<Vertex, Edge> OUT_EDGE_TRAVERSAL = new PureTraversal<>(__.outE().asAdmin());

    private PureTraversal<Vertex, ?> sourceVertexFilterTraversal = DEFAULT_VERTEX_FILTER_TRAVERSAL.clone();
    private PureTraversal<Vertex, ?> targetVertexFilterTraversal = DEFAULT_VERTEX_FILTER_TRAVERSAL.clone();
    private PureTraversal<Vertex, Edge> edgeTraversal = DEFAULT_EDGE_TRAVERSAL.clone();
    private PureTraversal<Edge, Number> distanceTraversal = DEFAULT_DISTANCE_TRAVERSAL.clone();
    private Number maxDistance;
    private boolean distanceEqualsNumberOfHops;
    private boolean includeEdges;
    private Integer antsNumber;
    private Number alpha;
    private Number rho;
    private Integer iterations;

    private static final Set<VertexComputeKey> VERTEX_COMPUTE_KEYS = new HashSet<>(Arrays.asList(
            VertexComputeKey.of(PATHS, true)));

    private final Set<MemoryComputeKey> memoryComputeKeys = new HashSet<>(Arrays.asList(
            MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true),
            MemoryComputeKey.of(STATE, Operator.assign, true, true),
            MemoryComputeKey.of(ITERATION, Operator.assign, true, true)));

    private SACOShortestPathVertexProgram() {

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

        if (configuration.containsKey(ALPHA))
            this.alpha = (Number) configuration.getProperty(ALPHA);
        else
            this.alpha = 2;

        if (configuration.containsKey(RHO))
            this.rho = (Number) configuration.getProperty(RHO);
        else
            this.rho = 0;

        this.distanceEqualsNumberOfHops = this.distanceTraversal.equals(DEFAULT_DISTANCE_TRAVERSAL);
        this.includeEdges = configuration.getBoolean(INCLUDE_EDGES, false);
        this.antsNumber = configuration.getInteger(ANTS_NUMBER, 1);
        this.iterations = configuration.getInteger(ITERATIONS, 10);

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
        configuration.setProperty(ANTS_NUMBER, this.antsNumber);
        if (this.maxDistance != null)
            configuration.setProperty(MAX_DISTANCE, maxDistance);
        if (this.alpha != null)
            configuration.setProperty(ALPHA, alpha);
        if (this.rho != null)
            configuration.setProperty(RHO, rho);
        if (this.iterations != null)
            configuration.setProperty(ITERATIONS, iterations);
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
    public VertexProgram<Ant> clone() {
        try {
            final SACOShortestPathVertexProgram clone = (SACOShortestPathVertexProgram) super.clone();
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
        memory.set(STATE, START);
        memory.set(ITERATION, 1);
    }

    @Override
    public void execute(final Vertex vertex, final Messenger<Ant> messenger, final Memory memory) {

        boolean voteToHalt = true;

        switch (memory.<Integer>get(STATE)) {
            case START:

                if (memory.isInitialIteration())
                    initializePheromone(vertex);

                if (isStartVertex(vertex))
                    spawnAnts(vertex, messenger);

                break;

            case SEARCH:

                final Iterator<Ant> antsIterator = messenger.receiveMessages();

                while (antsIterator.hasNext()) {
                    final Ant nextAnt = antsIterator.next();

                    if (isEndVertex(vertex)) {
                        final Map<Vertex, Pair<Number, ArrayList<Pair<Object, Number>>>> paths =
                                vertex.<Map<Vertex, Pair<Number, ArrayList<Pair<Object, Number>>>>>property(PATHS).orElseGet(HashMap::new);

                        nextAnt.extendPath(vertex);

                        nextAnt.removeCycles();

                        if (paths.containsKey(nextAnt.sourceVertex())) {
                            final Number currentShortestDistance = paths.get(nextAnt.sourceVertex()).getValue0();
                            final int cmp = NumberHelper.compare(nextAnt.distance(), currentShortestDistance);

                            if (cmp <= 0) {
                                if (cmp < 0) {
                                    paths.put(nextAnt.sourceVertex(), Pair.with(nextAnt.distance(), nextAnt.path()));
                                    vertex.property(VertexProperty.Cardinality.single, PATHS, paths);
                                } else {
                                    // if the path length is equal to the current shortest path's length
                                }
                            }
                        } else {
                            paths.put(nextAnt.sourceVertex(), Pair.with(nextAnt.distance(), nextAnt.path()));
                            vertex.property(VertexProperty.Cardinality.single, PATHS, paths);
                        }

                        nextAnt.setBackwardDirection();

                        moveAntBackward(nextAnt, vertex, messenger);

                        voteToHalt = false;
                    } else if (isStartVertex(vertex)) {
                        if (nextAnt.direction() == AntDirection.FROM_SOURCE) {
                            moveAntForward(nextAnt, vertex, messenger);
                            voteToHalt = false;
                        } else {
                            // LOGGER.info(nextAnt.toString());
                        }
                    } else {
                        if (nextAnt.direction() == AntDirection.FROM_SOURCE)
                            moveAntForward(nextAnt, vertex, messenger);
                        else
                            moveAntBackward(nextAnt, vertex, messenger);

                        voteToHalt = false;
                    }
                }

                break;

            case PHEROMONE_EVAPORATION:

                evaporatePheromone(vertex);

                break;

            case COLLECT_PATHS:

                collectShortestPaths(vertex, memory);

                break;

            default:
                break;
        }

        memory.add(VOTE_TO_HALT, voteToHalt);
    }

    @Override
    public boolean terminate(final Memory memory) {
        final boolean voteToHalt = memory.get(VOTE_TO_HALT);
        final int iteration = memory.get(ITERATION);

        memory.set(VOTE_TO_HALT, true);

        if (voteToHalt) {
            final int state = memory.get(STATE);

            if (state == START) {
                memory.set(STATE, SEARCH);

                return false;
            }

            if (state == SEARCH) {
                if (iteration + 1 <= iterations) {
                    memory.set(STATE, PHEROMONE_EVAPORATION);

                    memory.set(ITERATION, iteration + 1);
                } else
                    memory.set(STATE, COLLECT_PATHS);

                return false;
            }
            if (state == PHEROMONE_EVAPORATION) {
                memory.set(STATE, START);

                return false;
            }

            return true;
        } else
            return false;
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

    private void initializePheromone(final Vertex vertex) {
        final Traversal.Admin<Vertex, Edge> edgeTraversal = this.OUT_EDGE_TRAVERSAL.getPure();
        edgeTraversal.addStart(edgeTraversal.getTraverserGenerator().generate(vertex, edgeTraversal.getStartStep(), 1));

        while (edgeTraversal.hasNext()) {
            final Edge edge = edgeTraversal.next();

            edge.property(EDGE_PHEROMONE, 1);
        }
    }

    private List<Pair<Edge, Double>> calculateProbabilities(final Vertex vertex, final Edge predecessor) {
        final List<Pair<Edge, Double>> result = new ArrayList<>();

        final Traversal.Admin<Vertex, Edge> edgeTraversal = this.edgeTraversal.getPure();
        edgeTraversal.addStart(edgeTraversal.getTraverserGenerator().generate(vertex, edgeTraversal.getStartStep(), 1));

        List<Edge> edgesList = edgeTraversal.toList();

        Double probabilitiesSum = 0.0;

        Iterator<Edge> edgesIterator = edgesList.iterator();

        int predecessorIndex = 0;
        int edgeIndex = 0;

        if (edgesIterator.hasNext()) {
            while (edgesIterator.hasNext()) {
                final Edge edge = edgesIterator.next();
                final Double tau = Double.valueOf(edge.property(EDGE_PHEROMONE).value().toString());

                if ((!edge.equals(predecessor)) || (edgesList.size() <= 1))
                    probabilitiesSum += Math.pow(tau, alpha.doubleValue());
                else
                    if (predecessor != null)
                        predecessorIndex = edgeIndex;

                edgeIndex++;
            }

            int randomEdge = (int) (Math.random() * edgesList.size());

            if (predecessor != null)
                randomEdge = (randomEdge == predecessorIndex) ?
                        (edgesList.size() <= 1) ?
                                0 :
                                (randomEdge + 1 >= edgesList.size()) ?
                                        randomEdge - 1 : randomEdge + 1

                        : randomEdge;

            edgeIndex = 0;
            edgesIterator = edgesList.iterator();

            while (edgesIterator.hasNext()) {
                final Edge edge = edgesIterator.next();
                final Double tau = Double.valueOf(edge.property(EDGE_PHEROMONE).value().toString());

                Double edgeProbability = probabilitiesSum == 0 ?
                        edgeIndex == randomEdge ? 1 : 0
                        : Math.pow(tau, alpha.doubleValue()) / probabilitiesSum;

                if ((!edge.equals(predecessor)) || (edgesList.size() <= 1)) {
                    result.add(Pair.with(edge, edgeProbability));
                }

                edgeIndex++;
            }
        }

        return result;
    }

    private void spawnAnts(final Vertex vertex, final Messenger<Ant> messenger) {
        final List<Pair<Edge, Double>> edgesProbabilities = calculateProbabilities(vertex, null);

        if (!edgesProbabilities.isEmpty()) {
            for (int i = 0; i < antsNumber; i++) {
                final Ant newAnt = Ant.of(String.valueOf(i));

                if (!distanceEqualsNumberOfHops)
                    newAnt.setDistanceTraversal(this.distanceTraversal);

                Double choice = Math.random();
                Double cumulativeProbability = 0.0;

                Iterator<Pair<Edge, Double>> edgesIterator = edgesProbabilities.iterator();

                while ((edgesIterator.hasNext()) && (cumulativeProbability < choice )) {
                    final Pair<Edge, Double> edgeProbability = edgesIterator.next();

                    cumulativeProbability += edgeProbability.getValue1();

                    if (cumulativeProbability >= choice) {
                        Vertex otherV = edgeProbability.getValue0().inVertex();

                        if (otherV.equals(vertex))
                            otherV = edgeProbability.getValue0().outVertex();

                        newAnt.extendPath(vertex, edgeProbability.getValue0());

                        messenger.sendMessage(MessageScope.Global.of(otherV), newAnt);
                    }
                }
            }
        }
    }

    private void moveAntForward(final Ant ant, final Vertex vertex, final Messenger<Ant> messenger) {
        final List<Pair<Edge, Double>> edgesProbabilities = calculateProbabilities(vertex, ant.getLastEdge());

        if (!edgesProbabilities.isEmpty()) {

            Double choice = Math.random();
            Double cumulativeProbability = 0.0;

            Iterator<Pair<Edge, Double>> edgesIterator = edgesProbabilities.iterator();

            while ((edgesIterator.hasNext()) && (cumulativeProbability < choice)) {
                final Pair<Edge, Double> edgeProbability = edgesIterator.next();

                cumulativeProbability += edgeProbability.getValue1();

                if (cumulativeProbability >= choice) {
                    Vertex otherV = edgeProbability.getValue0().inVertex();

                    if (otherV.equals(vertex))
                        otherV = edgeProbability.getValue0().outVertex();

                    ant.extendPath(vertex, edgeProbability.getValue0());

                    messenger.sendMessage(MessageScope.Global.of(otherV), ant);
                }
            }
        }
    }

    private void moveAntBackward(final Ant ant, final Vertex vertex, final Messenger<Ant> messenger) {
        Edge backwardEdge = ant.getBackwardEdge();

        final Traversal.Admin<Vertex, Edge> edgeTraversal = DEFAULT_EDGE_TRAVERSAL.getPure();
        edgeTraversal.addStart(edgeTraversal.getTraverserGenerator().generate(vertex, edgeTraversal.getStartStep(), 1));

        List<Edge> edgesList = edgeTraversal.toList();

        Iterator<Edge> edgesIterator = edgesList.iterator();

        while (edgesIterator.hasNext()) {
            Edge edge = edgesIterator.next();

            if (edge.equals(backwardEdge)) {
                edge.property(EDGE_PHEROMONE,
                        Double.valueOf(edge.property(EDGE_PHEROMONE).value().toString()) + 1.0 / ant.distance().doubleValue());

                Vertex otherV = edge.inVertex();

                if (otherV.equals(vertex))
                    otherV = edge.outVertex();

                ant.moveBackward();

                messenger.sendMessage(MessageScope.Global.of(otherV), ant);

                break;
            }
        }
    }

    private void evaporatePheromone(final Vertex vertex) {
        final Traversal.Admin<Vertex, Edge> edgeTraversal = OUT_EDGE_TRAVERSAL.getPure();
        edgeTraversal.addStart(edgeTraversal.getTraverserGenerator().generate(vertex, edgeTraversal.getStartStep(), 1));

        List<Edge> edgesList = edgeTraversal.toList();

        Iterator<Edge> edgesIterator = edgesList.iterator();

        while (edgesIterator.hasNext()) {
            Edge edge = edgesIterator.next();

            edge.property(EDGE_PHEROMONE,
                    Double.valueOf(edge.property(EDGE_PHEROMONE).value().toString()) * (1 - rho.doubleValue()));
        }
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

        final VertexProperty<Map<Vertex, Pair<Number, ArrayList<Pair<Object, Number>>>>> pathProperty = vertex.property(PATHS);
        if (pathProperty.isPresent()) {
            if (isEndVertex(vertex)) {
                final Map<Vertex, Pair<Number, ArrayList<Pair<Object, Number>>>> paths = pathProperty.value();
                final List<Pair<Path, Number>> result = new ArrayList<>();

                for (final Pair<Number, ArrayList<Pair<Object, Number>>> pair : paths.values()) {
                    Path shortPath = ImmutablePath.make();

                    for (final Pair<Object, Number> pathElement : pair.getValue1())
                        shortPath = shortPath.extend(pathElement.getValue0(), Collections.emptySet());

                    if (this.distanceEqualsNumberOfHops ||
                            this.maxDistance == null ||
                            NumberHelper.compare(pair.getValue0(), this.maxDistance) <= 0) {
                        result.add(Pair.with(shortPath, pair.getValue0()));
                    }
                }

                memory.add(SHORTEST_PATHS, result);
            }

            pathProperty.remove();
        }
    }

    static class Ant {

        private String id;
        private AntDirection direction;
        private ArrayList<Pair<Object, Number>> antPath;
        private Number distance = 0;
        private boolean distanceEqualsNumberOfHops = true;
        private PureTraversal<Edge, Number> distanceTraversal;
        private int backwardEdge = 0;

        private Ant(String id) {
            this.id = id;
            this.antPath = new ArrayList<>();
            direction = AntDirection.FROM_SOURCE;
        }

        static Ant of(String id) {
            return new Ant(id);
        }

        AntDirection direction() {
            return direction;
        }

        Number distance() {
            return distance;
        }

        String id() {
            return id;
        }

        public void setBackwardDirection() {
            direction = AntDirection.FROM_DESTINATION;
            backwardEdge = antPath.size() - 2;
        }

        public Vertex sourceVertex()
        {
            Object vertex =  antPath.size() > 0 ? antPath.get(0).getValue0() : null;

            return vertex instanceof Vertex ? (Vertex) vertex : null;
        }

        public ArrayList<Pair<Object, Number>> path()
        {
            return this.antPath;
        }

        public void moveBackward() {
            backwardEdge = backwardEdge - 2;
        }

        public void setDistanceTraversal(PureTraversal<Edge, Number> distanceTraversal) {
            distanceEqualsNumberOfHops = false;
            this.distanceTraversal = distanceTraversal;
        }

        public void removeCycles() {
            ArrayList<Pair<Object, Number>> p = new ArrayList<>();

            Number newDistance = 0;
            boolean isCyclic = false;

            for (int i = 0; i < antPath.size(); ++i) {
                boolean inCycle = false;
                Integer lastCycleIndex = 0;

                if (antPath.get(i).getValue0() instanceof Vertex) {
                    for (int j = i + 1; j < antPath.size(); ++j) {
                        if ((antPath.get(j).getValue0() instanceof Vertex) && (antPath.get(i).getValue0().equals(antPath.get(j).getValue0()))) {
                            inCycle = true;
                            lastCycleIndex = j;
                            isCyclic = true;
                        }
                    }
                } else
                    newDistance = newDistance.intValue() + antPath.get(i).getValue1().intValue();

                p.add(new Pair<>(ReferenceFactory.detach(antPath.get(i).getValue0()), antPath.get(i).getValue1()));

                if (inCycle)
                    i = lastCycleIndex;
            }

            if (isCyclic) {
                antPath = p;
                distance = newDistance;
            }
        }

        protected Number getDistance(final Edge edge) {
            if (this.distanceEqualsNumberOfHops) return 1;
            final Traversal.Admin<Edge, Number> traversal = this.distanceTraversal.getPure();
            traversal.addStart(traversal.getTraverserGenerator().generate(edge, traversal.getStartStep(), 1));
            return traversal.tryNext().orElse(0);
        }

        private void extendPath(final Element... elements) {
            for (final Element element : elements) {
                if (element != null) {
                    Number edgeDistance = element instanceof Edge ? getDistance((Edge) element) : 0;
                    antPath.add(new Pair<>(ReferenceFactory.detach(element), edgeDistance));

                    distance = distance.intValue() + edgeDistance.intValue();
                }
            }
        }

        private Edge getLastEdge()
        {
            Object edge = antPath.size() > 0 ? antPath.get(antPath.size() - 1).getValue0() : null;

            return edge instanceof Edge ? (Edge) edge : null;
        }

        private Edge getBackwardEdge()
        {
            Object edge =  backwardEdge < antPath.size() ? antPath.get(backwardEdge).getValue0() : null;

            return edge instanceof Edge ? (Edge) edge : null;
        }

        @Override
        public String toString() {
            return "Ant " + id() + ": distance = " + distance + ", path = " + antPath.toString();
        }
    }

    enum AntDirection {
        FROM_SOURCE, FROM_DESTINATION;
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    @SuppressWarnings("WeakerAccess")
    public static final class Builder extends AbstractVertexProgramBuilder<Builder> {


        private Builder() {
            super(SACOShortestPathVertexProgram.class);
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

        public Builder alpha(final Number alpha) {
            if (null != alpha)
                this.configuration.setProperty(ALPHA, alpha);
            else
                this.configuration.clearProperty(ALPHA);
            return this;
        }

        public Builder rho(final Number rho) {
            if (null != rho)
                this.configuration.setProperty(RHO, rho);
            else
                this.configuration.clearProperty(RHO);
            return this;
        }

        public Builder iterations(final Integer iterations) {
            if (null != iterations)
                this.configuration.setProperty(ITERATIONS, iterations);
            else
                this.configuration.clearProperty(ITERATIONS);
            return this;
        }

        public Builder antsNumber(final Integer ants) {
            if (null == ants) throw Graph.Exceptions.argumentCanNotBeNull("antsNumber");
            this.configuration.setProperty(ANTS_NUMBER, ants);
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