from main import app, read_sql, simple_jsonify_df
from flask import Flask, request, jsonify
import functools

##These hold the HTTP Routing methods

@app.route("/uber_ride", methods=["POST"])
def uber_ride():
    content = request.json
    if "condition" in content:
        
        where_clause = content["condition"] #this is such bad code :P, anyone could do an sql injection here
    else:
        where_clause = ""
    sql_stmt = "select count(*) from uber {}".format(where_clause)
    results = read_sql(sql_stmt)
    return simple_jsonify_df(results)

@app.route("/citibike/latLng", methods=["POST"])
def citibike_lat_lng():
    content = request.json
    day = content["day"]
    month = content["month"]
    year = content["year"]
    start_lat = content["start"]["lat"]
    start_lng = content["start"]["lng"]
    end_lat = content["end"]["lat"]
    end_lng = content["end"]["lng"]
    delta= 0.07
    results = None
    
    while (results is None or len(results) == 0) and delta < 1.5:
        max_start_lat = start_lat + delta
        min_start_lat = start_lat - delta
        max_start_lng = start_lng + delta
        min_start_lng = start_lng - delta
        max_end_lat = end_lat + delta
        min_end_lat = end_lat - delta
        max_end_lng = end_lng + delta
        min_end_lng = end_lng - delta
        results = read_sql("select tripduration, starttime, stoptime, `start station name`, `end station name` from citibike where starttime > STR_TO_DATE('{}-{}-{}', \"%%m-%%d-%%Y\") and stoptime < STR_TO_DATE('{}-{}-{}', \"%%m-%%d-%%Y\") and `start station latitude` > {} and `start station latitude` < {} and `start station longitude` > {} and `start station longitude` < {} and `end station latitude` > {} and `end station latitude` < {} and `end station longitude` > {} and `end station longitude` < {} limit 500".format(month, day, year, month, int(day)+1, year, min_start_lat, max_start_lat, min_start_lng, max_start_lng, min_end_lat, max_end_lat, min_end_lng, max_end_lng, ))
        delta += 0.5

    return simple_jsonify_df(results)

@app.route("/uber_ride/latLng", methods=["POST"])
def uber_ride_lat_lng():
    content = request.json
    day = content["day"]
    month = content["month"]
    year = content["year"]
    start_lat = content["start"]["lat"]
    start_lng = content["start"]["lng"]
    end_lat = content["end"]["lat"]
    end_lng = content["end"]["lng"]
    delta= 0.07
    results = None
    
    while (results is None or len(results) == 0) and delta < 0.7:
        max_start_lat = start_lat + delta
        min_start_lat = start_lat - delta
        max_end_lat = end_lat + delta
        min_end_lat = end_lat - delta

        max_start_lng = start_lng + delta
        min_start_lng = start_lng - delta
        max_end_lng = end_lng + delta
        min_end_lng = end_lng - delta

        results = read_sql("SELECT day, month, year, start_lat, start_long, end_lat, end_long, speed_mph_mean, uber.start_junction_id as start_junction_id, uber.end_junction_id as end_junction_id from uber join (select junction_id as start_junction_id, osm_node_id as start_osm_node, latitude as start_lat, longitude as start_long from OSM_nodes) as start_osm_nodes on  start_osm_nodes.start_junction_id=uber.start_junction_id join (select junction_id as end_junction_id, osm_node_id as end_osm_node, latitude as end_lat, longitude as end_long from OSM_nodes) as end_osm_nodes on  end_osm_nodes.end_junction_id=uber.end_junction_id where day={} and month={} and year={} and start_lat > {} and start_lat < {} and end_lat > {} and end_lat < {} and start_long > {} and start_long < {} and end_long > {} and end_long < {} limit 5000".format(day, month, year, min_start_lat, max_start_lat, min_end_lat, max_end_lat, min_start_lng, max_start_lng, min_end_lng, max_end_lng))
        delta += 0.1

    
    return simple_jsonify_df(results)

@app.route("/osm_node", methods=["POST"])
def get_closest_node():
    content = request.json
    latitude = content["lat"]
    longitude = content["lng"]
    return simple_jsonify_df(read_sql("select * from OSM_nodes where not isnull(latitude) order by (latitude-{})*(latitude-{}) + (longitude-{})*(longitude-{}) asc limit 1".format(latitude, latitude, longitude, longitude)))

@app.route("/citibike", methods=["POST"])
def citibike():
    content = request.json
    if "condition" in where_clause:
        where_clause = content["condition"]
    else:
        where_clause = ""
    sql_stmt = "select * from citibike {}".format(where_clause)
    results = read_sql(sql_stmt)
    return simple_jsonify_df(results)


    
    

class Graph():
    def get_edges(self, start_node):
        return read_sql("SELECT * from edgelist where start_junction_id={}".format(start_node))
    def __init__(self, dataframe):
        """
        self.edges is a dict of all possible next nodes
        e.g. {'X': ['A', 'B', 'C', 'E'], ...}
        self.weights has all the weights between two nodes,
        with the two nodes as a tuple as the key
        e.g. {('X', 'A'): 7, ('X', 'B'): 2, ...}
        """
        self.edges = {}
        self.weights = {}

        for index, row in dataframe.iterrows():
            src = row["start_junction_id"]
            trg = row["end_junction_id"]
            weight = row["weight"]
            if src not in self.edges.keys():
                self.edges[src] = [trg]
            else:
                self.edges[src].append(trg)
            self.weights[(src, trg)] = weight
            
        for index, row in dataframe.iterrows():
            src = row["start_junction_id"]
            trg = row["end_junction_id"]
            weight = row["weight"]
            if trg not in self.edges.keys():
                self.edges[trg] = []
    def generate_junction_paths_weights(self):
        #TODO: Make this
        pass
    
@functools.lru_cache(10000)
def djikstra(graph, initial, end):
    # shortest paths is a dict of nodes
    # whose value is a tuple of (previous node, weight)
    shortest_paths = {initial: (None, 0)}
    current_node = initial
    visited = set()
    
    while current_node != end:
        visited.add(current_node)
        destinations = graph.edges[current_node]
        weight_to_current_node = shortest_paths[current_node][1]

        for next_node in destinations:
            weight = graph.weights[(current_node, next_node)] + weight_to_current_node
            if next_node not in shortest_paths:
                shortest_paths[next_node] = (current_node, weight)
            else:
                current_shortest_weight = shortest_paths[next_node][1]
                if current_shortest_weight > weight:
                    shortest_paths[next_node] = (current_node, weight)
        
        next_destinations = {node: shortest_paths[node] for node in shortest_paths if node not in visited}
        if not next_destinations:
            return "Route Not Possible"
        # next node is the destination with the lowest weight
        current_node = min(next_destinations, key=lambda k: next_destinations[k][1])
    
    # Work back through destinations in shortest path
    path = []
    while current_node is not None:
        path.append(current_node)
        next_node = shortest_paths[current_node][0]
        current_node = next_node
    # Reverse path
    path = path[::-1]
    return path 

edgeList = read_sql("select start_junction_id, end_junction_id, avg_speed as weight, start_lat, start_long, end_lat, weight as end_long from edgelist;")
graph = Graph(edgeList)

@app.route("/short_path_alg", methods=["POST"])
def shortest_path_algorithm():
    global graph
    content = request.json
    start_junction = content["start"]
    end_junction = content["end"]
    path = djikstra(graph, start_junction, end_junction)
    setStr = "("
    for junction_id in path:
        setStr += "'" + junction_id + "'" +", "
    setStr = setStr[:-2] + ")"

    edges = read_sql("select * from OSM_nodes where junction_id in {}".format(setStr))
    edges = edges.set_index("junction_id").loc[path]
    return simple_jsonify_df(edges)


def bikeTripInfo(path):
    #Assumptions:
    #1) All segments are 0.14 miles (https://www.nytimes.com/2006/09/17/nyregion/thecity/17fyi.html)
    #2) All bikes travel at a constant 10km/hr (around 6.21 mph) (https://www.treehugger.com/bikes/new-study-shows-urban-cycling-is-faster-than-driving.html)
    speed = 6.21
    distance = 0;
    for i in range(len(path)-1):
        distance += 0.14
    time = distance / speed
    return([distance, time*60])