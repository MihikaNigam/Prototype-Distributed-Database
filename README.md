# Prototype-Distributed-Database

Modern cloud databases should have high availability and be fault tolerant. Users want to be able to access their data at any time of the day. They also do not want their data to get lost. To ensure this it is necessary to make sure the database does not have a single point of failure. If one device goes down, we do not want all our data and/or the entire service to go down with it. To achieve this a distributed database is used.

As a proof of concept we built this Distributed Database that uses Raft and Gossip protocol w Ring Hashing to detect failed nodes, re-elect leaders when necessary, and it can distribute the data evenly as well as have data replication.
