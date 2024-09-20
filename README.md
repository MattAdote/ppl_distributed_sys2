# Pre-engagement Assessment

TCP Server Distributed System

## Problem spec

Design and build a distributed system.

Build a TCP server that can accept and hold a maximum of N clients (where N is configurable).
These clients are assigned ranks based on first-come-first-serve, i.e whoever connects first receives the next available high rank. Ranks are from 0–N, 0 being the highest rank.

Clients can send to the server commands that the server distributes among the clients. Only a client with a lower rank can execute a command of a higher rank client. Higher rank clients cannot execute commands by lower rank clients, so these commands are rejected. The command execution can be as simple as the client printing to console that command has been executed.

If a client disconnects the server should re-adjust the ranks and promote any client that needs to be promoted not to leave any gaps in the ranks.

## Solution Specification

### System Architecture

The requirements of the distributed system dictate a star topology in which the server and clients are
bi-directional nodes i.e. both server and clients can play the server/client role.
The TCP Server is the central node in this architecture and co-ordinates the functioning of the system.
Thus, it playes the role of HOST / MASTER while the TCP Clients play the role of WORKER nodes

#### Key Points:
+ MASTER node maintains three queues:
	1. queue_connected_clients
		+ tracks all workers available in the system
	2. queue_busy_clients
		+ tracks workers that are known to be executing commands issued by MASTER
	3. queue_client_heartbeats
		+ tracks timestamps of connected clients to determine whether any have disconnected

+ ZeroMQ library is used to create the distributed system with [NetMQ](https://github.com/zeromq/netmq) used

### How to run the application
The tcp ports used are:
+ MASTER node
	+ ZMQ REQ Socket: 5555 (debug | release builds)
+ WORKER node
	+ ZMQ REQ Socket: 5556 (debug build)
	+ ZMQ REQ Socket: 5557 (release build)

To launch the MASTER node:
+ navigate to `../DS_Host/bin/[Debug|Release]/net8.0/DS_Host.exe`

To launch the WORKER node:
+ navigate to `../DS_Worker/bin/[Debug|Release]/net8.0/DS_Worker.exe`

You may launch more than one worker on a single PC provided that each worker listens on a different port.

### Running the demo application
+ navigate to the `../builds/` directory and extract DS_Host.zip and DS_Worker.zip to a location of your choice.
+ then navigate to `../[debug|release]/net8.0/DS_Worker.exe` for WORKER and `../net8.0/DS_Host.exe` for MASTER

This demo was built using Visual Studio Community 2022 