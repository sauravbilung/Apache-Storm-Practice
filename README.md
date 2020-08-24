# Apache-Storm-Practice
Distributed stream processing using Apache Storm.

### Required
1) Apache Storm  
2) Apache Zookeepeer  
3) JDK 1.8  
4) Any JAVA IDE

### For Remote topologies :  
1) Setup up storm and zookeeper.
2) Setup up the environment variables for them.
3) Start the Zookeeper : cd to the zookeeper bin directory and run ```./zkServer.sh start```  
4) Start the storm cluster by running the commands : ```storm nimbus -> storm supervisor -> storm ui -> storm drpc```  
5) For the remote topology examples build the jar by keeping the scope as ```provided``` for ```storm-core``` dependency.  
6) Submit the topology to the storm cluster by : ```storm jar "path to the jar" package_name.TopologyMainFunction.```    
7) In DRPC examples submit the DRPC client request by : ```storm jar "path to the jar" package_name.DRPCClientMainFunction.```
8) To stop zookeeper : cd to the zookeeper bin directory and run ```./zkServer.sh stop```

### For running in topologies programed local cluster :   
1) Simply run the topology main function.


### Note:
1) The output is very verbose so we have to search the logs/console for the output.
2) Storm and Zookeeper will throw error if they are not configured properly. 
3) To start storm UI: ```127.0.0.1/8080``` (ip:port that you have configured)
4) If topology is running remotely . Perform the various operations like kill,start,etc easily in the ```storm ui``` instead of the terminal.


## Bugs in Direct Grouping. Will be fixed soon.




