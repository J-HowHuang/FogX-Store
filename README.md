# 🦊 🗃️ FogX-Store 
FogX-Store is a dataset store service that collects and serves large robotics datasets. 

Users of [Fog-X](https://github.com/KeplerC/fog_x/tree/main), machine learning practitioners in robotics, can solely rely on FogX-Store to get datasets from different data source (FogX-Store is the Hub!), and perform analytic workloads without the need to manage the data flow themselves.

<img width="827" alt="截屏2024-09-23 13 14 57" src="https://github.com/user-attachments/assets/d12e0ad2-022c-4c2a-bc84-a384e8726089">

## Components
### Skulk
Skulk is a metadata service, like "catalog" in the database world, it provides information of every dataset in the system such as:
* The location of a partition from a dataset
* Schema of a dataset
* Statistics about each partition or data host

Skulk as a catalog, it exposes RESTful API for users to do management level operations, namely, update dataset metadata. Users could be
* System admin
* Collector Fox running on a robot host

It also hosts an Apache Arrow Flight server, to which client can request dataset by sending a skulk query. Skulk will then response with a list of endpoints where client can then request the actual dataset data. Each endpoint is essentially a Predator Fox who runs another Arrow flight server, and also the one who possesses the actual dataset data. Arrow flight framework is used here between skulk and client, client and Predator Fox.

### Predator Fox
Predator Fox runs on a data node, in general, a robot or wherever a dataset is produced and persisted. 
The job of the Predator Fox is to process dataset query requests, send the query result back to clients. It runs a Apache Arrow Flight server as a framework and interface with client.

Predator Fox relies on Collector Fox to curate data from various data source into a common designated database, from which the Predator Fox can query.

### Collector Fox


## Integration Test

```
docker compose build
docker compose up
```

This docker compose configuration brings up
* 1 Skulk server
* 2 Data nodes (each contains 1 Predator and 1 Collector Fox)
* 1 Container that runs the test script
* 1 Dummy container with the same environment as the test script container for debugging

#### Develop
We have a limitation that each data node has to have a public IP to access. Therefore, sometimes we need a container within the same docker network to access the docker network in the docker compose. (it seems to be possible to access the docker bridge network from host machine but might be some problem with MacOS which I am not sure.)


```bash
# Run this to log into the shell with the dev environment
docker exec -it fogx-store-test_client_node-1 /bin/bash
```
