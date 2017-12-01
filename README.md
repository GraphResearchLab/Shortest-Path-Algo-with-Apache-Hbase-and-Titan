### Titan Hbase SSSP

##### Task: Calculate the shortest path for all the edges in a Bi-Directional graph.
<br>
<b>Description:</b> Consider the following scenario, a country has lot of villages , each village represents a node.<br>Create a dummy data-set representing N villages, say N=50 or 100 for now.<br>
<b>properties of villages are:</b>

    1. Village-Name:
    2. No of roads Connecting to Other Villages. (i.e Edges).
    3. Length of each Road.
    4. Names of Neighboring Villages respective to a Road(Edge).

You need to store all this data in Hbase. So a common way of storing this type of data in Hbase is, each row represents a Village.<br/>
Read the Village Data from Hbase, create the vertices and edges and Populate the graph in a Graph DB of your choice. <br/>
Implement the Shortest Path Algorithm and save the results back to Hbase

<b>Technologies To Be Used:</b>

    1. Hadoop Version of 1.2 or higher.
    2. Hbase Version of 0.94 or higher.
    3. Graph DB of your choice - Giraph, Titan, Neo4J
    4. Development Platform - Java

# My Solution:

    1) I have set up Hadoop and HBase on single node. Titan used HBase as backend storage.
    2) I have written Java Code with HBase Client API to upload graph data from local file system to HBase database.
    3) I have written Java Code with HBase & Titan Client API to load graph from HBase and implemented SSSP with the same. The output is stored back to HBase.

#### Project With: Infinilitics.com
