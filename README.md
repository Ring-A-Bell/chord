# Distributed Hash Table using Chord

This README provides an overview and usage instructions for the Chord system, which is based on the original research paper, implemented in Python.

## Overview
The implemented Chord system utilizes Python 3 and follows the specifications outlined in the original Chord paper. The system allows for the creation of a distributed hash table (DHT) network where nodes can join, populate data, and query values efficiently.

---

## Functionality

- **Node Joining**: Nodes can join the network using a system-assigned port number.
- **Data Population**: Data can be populated into the DHT using the `chord_populate.py` script.
- **Querying**: Users can query the network for values associated with specific keys using the `chord_query.py` script.
- **Finger Table Updates**: The system rigorously updates all finger tables affected by newly joined or removed nodes, as described in Section 4 of the paper.
- **Data Set**: Each row from a National Football League passing statistics file is stored in the DHT. Duplicates are handled by allowing the last row to win.
- **Extra Credit**: There is an opportunity for new nodes to take over necessary keys and associated data when joining an existing network with data already in the DHT.

## Files

The implementation consists of the following files:

1. *chord_node.py*: Script for joining a new node into the Chord network and listening for incoming connections.
2. *chord_populate.py*: Script for populating data into the Chord DHT. Requires the port number of an existing node and the filename of the data file.
3. *chord_query.py*: Script for querying values from the Chord DHT. Requires the port number of an existing node and a key.

## Usage

#### chord_node.py

`python chord_node.py <port_number>`

`<port_number>`: Port number of an existing node (or 0 to start a new network).

#### chord_populate.py
`python chord_populate.py <existing_node_port> <data_filename>`

`<existing_node_port>`: Port number of an existing node in the network.

`<data_filename>`: Filename of the data file to be populated into the DHT.

#### chord_query.py
`python chord_query.py <existing_node_port> <key>`

`<existing_node_port>`: Port number of an existing node in the network.

`<key>`: Key for querying the associated value from the DHT.

---

## Requirements
* Python 3
* Standard libraries (socket, threading, pickle)

## Notes
* For testing purposes, the implementation uses a modulo arithmetic approach with a fixed value of *M*
* Handling deadlocks: Dispatching RPC requests in threads is implemented to handle potential deadlocks.

## Flaws in the Paper
The implementation addresses certain bugs found in the paper's pseudocode, particularly in the `update_others()` and `update_finger_table(s, i)` methods.

## References
Original Chord Paper: [Chord: A Scalable Peer-to-peer Lookup Service for Internet Applications](https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf) by Stoica, Morris, Karger, Kaashoek, and Balakrishna (2001)
