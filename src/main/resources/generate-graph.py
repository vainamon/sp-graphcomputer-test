import networkx as nx
import random
import numpy

dgm = nx.dorogovtsev_goltsev_mendes_graph(9)

print("Nodes:", nx.number_of_nodes(dgm), " Edges:", nx.number_of_edges(dgm), "Diameter:", nx.diameter(dgm))

periphery = nx.periphery(dgm)
pmax = 0
smax = 0
tmax = 0

for source in periphery:
    for target in periphery:
        spath = nx.shortest_path_length(dgm, source, target)

        if pmax < spath:
            pmax = spath
            smax = source
            tmax = target


label_attr = {smax: {'source': 'A'}, tmax: {'target': 'B'}}
nx.set_node_attributes(dgm, label_attr)

for e in nx.edges(dgm):
    dgm[e[0]][e[1]]['weight'] = random.randint(1, 100)

print("Path from A to B: ", nx.single_source_dijkstra(dgm, source=smax, target=tmax))
print("Avg. degree", numpy.average(list(zip(*nx.degree(dgm)))[1]))

nx.write_graphml(dgm, "dgm9.xml")
