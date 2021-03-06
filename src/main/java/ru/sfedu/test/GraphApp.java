    package ru.sfedu.test;

    import java.util.List;
    import java.util.Map;
    import java.util.Optional;

    import org.apache.commons.configuration.Configuration;
    import org.apache.commons.configuration.ConfigurationException;
    import org.apache.commons.configuration.PropertiesConfiguration;
    import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
    import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
    import org.apache.tinkerpop.gremlin.structure.Direction;
    import org.apache.tinkerpop.gremlin.structure.Graph;
    import org.apache.tinkerpop.gremlin.structure.Vertex;
    import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
    import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
    import org.apache.tinkerpop.gremlin.process.traversal.Path;
    import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;
    import org.javatuples.Pair;

    public class GraphApp {
        private static final Logger LOGGER = LoggerFactory.getLogger(GraphApp.class);

        protected String propFileName;
        protected Configuration conf;
        protected Graph graph;
        protected GraphTraversalSource g;
        protected boolean supportsTransactions;
        protected boolean supportsSchema;

        /**
         * Constructs a graph app using the given properties.
         * @param fileName location of the properties file
         */
        public GraphApp(final String fileName) {
            propFileName = fileName;
        }

        /**
         * Opens the graph instance. If the graph instance does not exist, a new
         * graph instance is initialized.
         */
        public GraphTraversalSource openGraph() throws ConfigurationException {
            LOGGER.info("opening graph");
            conf = new PropertiesConfiguration(propFileName);
            graph = GraphFactory.open(conf);
            g = graph.traversal();
            return g;
        }

        /**
         * Closes the graph instance.
         */
        public void closeGraph() throws Exception {
            LOGGER.info("closing graph");
            try {
                if (g != null) {
                    g.close();
                }
                if (graph != null) {
                    graph.close();
                }
            } finally {
                g = null;
                graph = null;
            }
        }

        /**
         * Drops the graph instance. The default implementation does nothing.
         */
        public void dropGraph() throws Exception {
        }

        /**
         * Creates the graph schema. The default implementation does nothing.
         */
        public void createSchema() {
        }

        /**
         * Adds the vertices, edges, and properties to the graph.
         */
        public void createTestGraphElements() {
            try {
                // naive check if the graph was previously created
                if (g.V().has("label", "A").hasNext()) {
                    if (supportsTransactions) {
                        g.tx().rollback();
                    }
                    return;
                }
                LOGGER.info("creating elements");

                final Vertex A = g.addV("vertex").property("label", "A").next();
                final Vertex B = g.addV("vertex").property("label", "B").next();
                final Vertex C = g.addV("vertex").property("label", "C").next();
                final Vertex D = g.addV("vertex").property("label", "D").next();
                final Vertex E = g.addV("vertex").property("label", "E").next();
                final Vertex F = g.addV("vertex").property("label", "F").next();
                final Vertex G = g.addV("vertex").property("label", "G").next();
                final Vertex H = g.addV("vertex").property("label", "H").next();
                final Vertex I = g.addV("vertex").property("label", "I").next();

                g.V(A).as("a").V(B).addE("adjacent").property("distance", 7).from("a").next();
                g.V(A).as("a").V(C).addE("adjacent").property("distance", 10).from("a").next();

                g.V(B).as("a").V(G).addE("adjacent").property("distance", 27).from("a").next();
                g.V(B).as("a").V(F).addE("adjacent").property("distance", 9).from("a").next();

                g.V(C).as("a").V(F).addE("adjacent").property("distance", 8).from("a").next();
                g.V(C).as("a").V(E).addE("adjacent").property("distance", 31).from("a").next();

                g.V(F).as("a").V(H).addE("adjacent").property("distance", 11).from("a").next();

                g.V(E).as("a").V(D).addE("adjacent").property("distance", 32).from("a").next();

                g.V(G).as("a").V(I).addE("adjacent").property("distance", 15).from("a").next();

                g.V(H).as("a").V(D).addE("adjacent#1").property("distance", 17).from("a").next();
                g.V(H).as("a").V(I).addE("adjacent").property("distance", 15).from("a").next();

                //g.V(D).as("a").V(H).addE("adjacent#2").property("distance", 17).from("a").next();
                g.V(D).as("a").V(I).addE("adjacent").property("distance", 21).from("a").next();

                if (supportsTransactions) {
                    g.tx().commit();
                }

            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                if (supportsTransactions) {
                    g.tx().rollback();
                }
            }
        }

        /**
         * Runs some traversal queries to get data from the graph.
         */
        public void readTestGraphElements() {
            try {
                if (g == null) {
                    return;
                }

                LOGGER.info("reading elements");

                final Optional<Map<Object, Object>> v = g.V().has("label", "A").valueMap().tryNext();
                if (v.isPresent()) {
                    LOGGER.info(v.get().toString());
                } else {
                    LOGGER.warn("A not found");
                }

            } finally {
                // the default behavior automatically starts a transaction for
                // any graph interaction, so it is best to finish the transaction
                // even for read-only graph query operations
                if (supportsTransactions) {
                    g.tx().rollback();
                }
            }
        }

        public void createGeneratedGraphElements() {
            try {
                // naive check if the graph was previously created
                if (g.V().has("source", "A").hasNext()) {
                    if (supportsTransactions) {
                        g.tx().rollback();
                    }
                    return;
                }
                LOGGER.info("creating elements");

                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

                g.io(classLoader.getResource("dgm9.xml").getFile()).read().iterate();

                LOGGER.info("Graph: nodes - " + g.V().count().next() + "; edges - " + g.E().count().next());

                if (supportsTransactions) {
                    g.tx().commit();
                }

            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                if (supportsTransactions) {
                    g.tx().rollback();
                }
            }
        }

        public void readGeneratedGraphElements() {
            try {
                if (g == null) {
                    return;
                }

                LOGGER.info("reading elements");

                final Optional<Map<Object, Object>> v = g.V().has("source", "A").valueMap().tryNext();
                if (v.isPresent()) {
                    LOGGER.info(v.get().toString());
                } else {
                    LOGGER.warn("A not found");
                }

            } finally {
                // the default behavior automatically starts a transaction for
                // any graph interaction, so it is best to finish the transaction
                // even for read-only graph query operations
                if (supportsTransactions) {
                    g.tx().rollback();
                }
            }
        }

        /**
         * Deletes elements from the graph structure. When a vertex is deleted,
         * its incident edges are also deleted.
         */
        public void deleteTestGraphElements() {
            try {
                if (g == null) {
                    return;
                }
                LOGGER.info("deleting elements");
                // note that this will succeed whether or not H exists
                g.V().has("label", "H").drop().iterate();
                if (supportsTransactions) {
                    g.tx().commit();
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                if (supportsTransactions) {
                    g.tx().rollback();
                }
            }
        }

        public void runTestGraphShortestPathComputer() {
            try {
                if (g == null) {
                    return;
                }
                LOGGER.info("run shortest path");

                StandardShortestPathVertexProgram spvp = StandardShortestPathVertexProgram.build()
                        .includeEdges(true)
                        .distanceProperty("distance")
                        .source(__.has("label", "A"))
                        .target(__.has("label", "D"))
                        .edgeDirection(Direction.BOTH)
                        .create();

                ComputerResult result = graph.compute().program(spvp).submit().get();

                List<Pair<Path, Number>> paths = result.memory().get(StandardShortestPathVertexProgram.SHORTEST_PATHS);

                LOGGER.info("Runtime = " + result.memory().getRuntime() + "ms; iteration: " + result.memory().getIteration());
                LOGGER.info("Path's count = " + paths.size());
                LOGGER.info("Path 0: " + paths.get(0).getValue0().toString());

                Integer distance = paths.get(0).getValue0().stream().filter(re -> re.getValue0() instanceof ReferenceEdge)
                        .mapToInt(re -> ((Integer) g.E(re.getValue0()).next().properties("distance").next().value()))
                        .sum();

                LOGGER.info("Path's distance: " + distance + "; " + paths.get(0).getValue1());

                /*paths.forEach(p -> {LOGGER.info("Path " + paths.indexOf(p));
                        p.getValue0().forEach(re -> LOGGER.info(re instanceof  ReferenceVertex ?
                            g.V(re).next().properties("label").next().toString()
                            : g.E(re).next().properties("distance").next().toString() + " " + g.E(re).next().toString())); });*/

                SACOShortestPathVertexProgram sacospvp = SACOShortestPathVertexProgram.build()
                        .includeEdges(true)
                        .distanceProperty("distance")
                        .source(__.has("label", "A"))
                        .target(__.has("label", "D"))
                        .edgeDirection(Direction.BOTH)
                        .antsNumber(6)
                        .rho(0.02)
                        .iterations(5)
                        .create();

                result = graph.compute().program(sacospvp).submit().get();
                paths = result.memory().get(SACOShortestPathVertexProgram.SHORTEST_PATHS);

                LOGGER.info("SACO Ant runtime = " + result.memory().getRuntime() + "ms; iteration: " + result.memory().getIteration());
                LOGGER.info("SACO Ant path's count = " + paths.size());
                LOGGER.info("SACO Ant path 0: " + paths.get(0).getValue0().toString());

                distance = paths.get(0).getValue0().stream().filter(re -> re.getValue0() instanceof ReferenceEdge)
                        .mapToInt(re -> ((Integer) g.E(re.getValue0()).next().properties("distance").next().value()))
                        .sum();

                LOGGER.info("SACO Ant path's distance: " + distance + "; " + paths.get(0).getValue1());

                ACSShortestPathVertexProgram acsspvp = ACSShortestPathVertexProgram.build()
                        .includeEdges(true)
                        .distanceProperty("distance")
                        .source(__.has("label", "A"))
                        .target(__.has("label", "D"))
                        .edgeDirection(Direction.BOTH)
                        .antsNumber(6)
                        .rho(0.02)
                        .q0(0.5)
                        .tau0(0.1)
                        .iterations(5)
                        .create();

                result = graph.compute().program(acsspvp).submit().get();
                paths = result.memory().get(ACSShortestPathVertexProgram.SHORTEST_PATHS);

                LOGGER.info("ACS Ant runtime = " + result.memory().getRuntime() + "ms; iteration: " + result.memory().getIteration());
                LOGGER.info("ACS Ant path's count = " + paths.size());
                LOGGER.info("ACS Ant path 0: " + paths.get(0).getValue0().toString());

                distance = paths.get(0).getValue0().stream().filter(re -> re.getValue0() instanceof ReferenceEdge)
                        .mapToInt(re -> ((Integer) g.E(re.getValue0()).next().properties("distance").next().value()))
                        .sum();

                LOGGER.info("ACS Ant path's distance: " + distance + "; " + paths.get(0).getValue1());

                MMASShortestPathVertexProgram mmasspvp = MMASShortestPathVertexProgram.build()
                        .includeEdges(true)
                        .distanceProperty("distance")
                        .source(__.has("label", "A"))
                        .target(__.has("label", "D"))
                        .edgeDirection(Direction.BOTH)
                        .antsNumber(6)
                        .rho(0.98)
                        .q0(0.0)
                        .maxfactor(5)
                        .iterations(5)
                        .create();

                result = graph.compute().program(mmasspvp).submit().get();
                paths = result.memory().get(MMASShortestPathVertexProgram.SHORTEST_PATHS);

                LOGGER.info("MMAS Ant runtime = " + result.memory().getRuntime() + "ms; iteration: " + result.memory().getIteration());
                LOGGER.info("MMAS Ant path's count = " + paths.size());
                LOGGER.info("MMAS Ant path 0: " + paths.get(0).getValue0().toString());

                distance = paths.get(0).getValue0().stream().filter(re -> re.getValue0() instanceof ReferenceEdge)
                        .mapToInt(re -> ((Integer) g.E(re.getValue0()).next().properties("distance").next().value()))
                        .sum();

                LOGGER.info("MMAS Ant path's distance: " + distance + "; " + paths.get(0).getValue1());

                if (supportsTransactions) {
                    g.tx().commit();
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                if (supportsTransactions) {
                    g.tx().rollback();
                }
            }
        }

        public void runGeneratedGraphShortestPathComputer() {
            try {
                if (g == null) {
                    return;
                }
                LOGGER.info("run shortest path");

                StandardShortestPathVertexProgram spvp = StandardShortestPathVertexProgram.build()
                        .includeEdges(true)
                        .distanceProperty("weight")
                        .source(__.has("source", "A"))
                        .target(__.has("target", "B"))
                        .edgeDirection(Direction.BOTH)
                        .create();

                ComputerResult result = graph.compute().program(spvp).submit().get();

                List<Pair<Path, Number>> paths = result.memory().get(StandardShortestPathVertexProgram.SHORTEST_PATHS);

                LOGGER.info("Runtime = " + result.memory().getRuntime() + "ms; iteration: " + result.memory().getIteration());
                LOGGER.info("Path's count = " + paths.size());
                //LOGGER.info("Path 0: " + paths.get(0).getValue0().toString());

                Long distance = paths.get(0).getValue0().stream().filter(re -> re.getValue0() instanceof ReferenceEdge)
                        .mapToLong(re -> ((Long) g.E(re.getValue0()).next().properties("weight").next().value()))
                        .sum();

                LOGGER.info("Path's distance: " + distance + "; " + paths.get(0).getValue1());

                /*paths.forEach(p -> {LOGGER.info("Path " + paths.indexOf(p));
                        p.getValue0().forEach(re -> LOGGER.info(re instanceof  ReferenceVertex ?
                            g.V(re).next().properties("label").next().toString()
                            : g.E(re).next().properties("distance").next().toString() + " " + g.E(re).next().toString())); });*/

                SACOShortestPathVertexProgram sacospvp = SACOShortestPathVertexProgram.build()
                        .includeEdges(true)
                        .distanceProperty("weight")
                        .source(__.has("source", "A"))
                        .target(__.has("target", "B"))
                        .edgeDirection(Direction.BOTH)
                        .antsNumber(6)
                        .rho(0.02)
                        .iterations(5)
                        .create();

                result = graph.compute().program(sacospvp).submit().get();
                paths = result.memory().get(SACOShortestPathVertexProgram.SHORTEST_PATHS);

                LOGGER.info("SACO Ant runtime = " + result.memory().getRuntime() + "ms; iteration: " + result.memory().getIteration());
                LOGGER.info("SACO Ant path's count = " + paths.size());
                //LOGGER.info("SACO Ant path 0: " + paths.get(0).getValue0().toString());

                distance = paths.get(0).getValue0().stream().filter(re -> re.getValue0() instanceof ReferenceEdge)
                        .mapToLong(re -> ((Long) g.E(re.getValue0()).next().properties("weight").next().value()))
                        .sum();

                LOGGER.info("SACO Ant path's distance: " + distance + "; " + paths.get(0).getValue1());

                List<Pair<Integer, Double>> ratios = result.memory().get(SACOShortestPathVertexProgram.CYCLE_PATH_MAX_RATIOS);

                for (Pair<Integer, Double> p : ratios) {
                    LOGGER.info("Iteration " + p.getValue0() + "; ratio " + p.getValue1());
                }

                ACSShortestPathVertexProgram acsspvp = ACSShortestPathVertexProgram.build()
                        .includeEdges(true)
                        .distanceProperty("weight")
                        .source(__.has("source", "A"))
                        .target(__.has("target", "B"))
                        .edgeDirection(Direction.BOTH)
                        .antsNumber(6)
                        .rho(0.02)
                        .alpha(0.01)
                        .q0(0.1)
                        .tau0(0.1)
                        .iterations(5)
                        .create();

                result = graph.compute().program(acsspvp).submit().get();
                paths = result.memory().get(ACSShortestPathVertexProgram.SHORTEST_PATHS);

                LOGGER.info("ACS Ant runtime = " + result.memory().getRuntime() + "ms; iteration: " + result.memory().getIteration());
                LOGGER.info("ACS Ant path's count = " + paths.size());
                //LOGGER.info("ACS Ant path 0: " + paths.get(0).getValue0().toString());

                if (paths.size() > 0)
                    distance = paths.get(0).getValue0().stream().filter(re -> re.getValue0() instanceof ReferenceEdge)
                        .mapToLong(re -> ((Long) g.E(re.getValue0()).next().properties("weight").next().value()))
                        .sum();
                else
                    distance = 0L;

                LOGGER.info("ACS Ant path's distance: " + distance + "; " + paths.get(0).getValue1());

                ratios = result.memory().get(ACSShortestPathVertexProgram.CYCLE_PATH_MAX_RATIOS);

                for (Pair<Integer, Double> p : ratios) {
                    LOGGER.info("Iteration " + p.getValue0() + "; ratio " + p.getValue1());
                }

                MMASShortestPathVertexProgram mmasspvp = MMASShortestPathVertexProgram.build()
                        .includeEdges(true)
                        .distanceProperty("weight")
                        .source(__.has("source", "A"))
                        .target(__.has("target", "B"))
                        .edgeDirection(Direction.BOTH)
                        .antsNumber(6)
                        .rho(0.98)
                        .q0(0.0)
                        .maxfactor(5)
                        .iterations(5)
                        .create();

                result = graph.compute().program(mmasspvp).submit().get();
                paths = result.memory().get(MMASShortestPathVertexProgram.SHORTEST_PATHS);

                LOGGER.info("MMAS Ant runtime = " + result.memory().getRuntime() + "ms; iteration: " + result.memory().getIteration());
                LOGGER.info("MMAS Ant path's count = " + paths.size());
                //LOGGER.info("MMAS Ant path 0: " + paths.get(0).getValue0().toString());

                if (paths.size() > 0)
                    distance = paths.get(0).getValue0().stream().filter(re -> re.getValue0() instanceof ReferenceEdge)
                        .mapToLong(re -> ((Long) g.E(re.getValue0()).next().properties("weight").next().value()))
                        .sum();
                else
                    distance = 0L;

                LOGGER.info("MMAS Ant path's distance: " + distance + "; " + paths.get(0).getValue1());

                ratios = result.memory().get(MMASShortestPathVertexProgram.CYCLE_PATH_MAX_RATIOS);

                for (Pair<Integer, Double> p : ratios) {
                    LOGGER.info("Iteration " + p.getValue0() + "; ratio " + p.getValue1());
                }

                if (supportsTransactions) {
                    g.tx().commit();
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                if (supportsTransactions) {
                    g.tx().rollback();
                }
            }
        }

        public void runApp() {
            try {
                // open and initialize the graph
                openGraph();

                // define the schema before loading data
                if (supportsSchema) {
                    createSchema();
                }

                // build the graph structure
                //createTestGraphElements();
                createGeneratedGraphElements();
                // read to see they were made
                //readTestGraphElements();
                readGeneratedGraphElements();

                //runTestGraphShortestPathComputer();
                runGeneratedGraphShortestPathComputer();

                // delete some graph elements
                //deleteTestGraphElements();

                //runTestGraphShortestPathComputer();

                // close the graph
                closeGraph();
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }
