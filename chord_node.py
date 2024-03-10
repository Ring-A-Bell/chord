"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Aditya Ganti
:Assignment: Lab 4
:EC Not attempted
"""
import hashlib
import pickle
import socket
import socketserver
import sys
import threading

from typing import Union

# Feel free to change the values of M and BASE_PORT for testing purposes
BACKLOG = 100
BASE_PORT = 53777
BUFF_SIZE = 4096
M = 3
NODES = 2 ** M
POSSIBLE_HOSTS = ['localhost']
POSSIBLE_PORTS = list(range(1, 2**16))


class ModRange(object):
    """
    Range-like object that wraps around 0 at some divisor using modulo arithmetic.
    """

    def __init__(self, start, stop, divisor):
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor
        # we want to use ranges to make things speedy, but if it wraps around the 0 node, we have to use two
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        elif self.stop == 0:
            self.intervals = (range(self.start, self.divisor),)
        else:
            self.intervals = (range(self.start, self.divisor), range(0, self.stop))

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.stop, self.divisor)

    def __contains__(self, id_):
        """ Is the given id within this finger's interval? """
        for interval in self.intervals:
            if id_ in interval:
                return True
        return False

    def __len__(self):
        total = 0
        for interval in self.intervals:
            total += len(interval)
        return total

    def __iter__(self):
        return ModRangeIter(self, 0, -1)


class ModRangeIter(object):
    """ Iterator class for ModRange """
    def __init__(self, mr, i, j):
        self.mr, self.i, self.j = mr, i, j

    def __iter__(self):
        return ModRangeIter(self.mr, self.i, self.j)

    def __next__(self):
        if self.j == len(self.mr.intervals[self.i]) - 1:
            if self.i == len(self.mr.intervals) - 1:
                raise StopIteration()
            else:
                self.i += 1
                self.j = 0
        else:
            self.j += 1
        return self.mr.intervals[self.i][self.j]


class FingerEntry(object):
    """
    Row in a finger table.
    """

    def __init__(self, n, k, node_=None):
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (n + 2 ** (k - 1)) % NODES
        self.next_start = (n + 2 ** k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node_

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.next_start, self.node)

    def __contains__(self, id_):
        """ Is the given id within this finger's interval? """
        return id_ in self.interval


class ChordNode:
    def __init__(self, id_: int = 10, join_through: int = None) -> None:
        """
        Initializes the ChordNode object, and declares/initializes the properties required for a node.
        Creates a threaded server that keeps listening for RPC calls, and also calls the join function.

        :param id_: ID which the current node should assume
        :param join_through: The ID of the node through which the new node has joined
        """
        self.node_map = None
        self.listen_address = self.lookup_node(id_)
        self.keys = dict()
        self.node = self
        self.bucket_data = dict()
        self.node_id = self.generate_node_id(id_)  # Unique ID for this node
        # self.node_id = self.hash(self.listen_address[0] + str(self.listen_address[1]))
        print(f"Node ID number for this node is : {self.node_id}")
        self.finger = [None] + [FingerEntry(self.node_id, k) for k in range(1, M+1)]
        self._successor = self.finger[1].node
        self._predecessor = None
        print(f"Finger table for node: {self.node_id} after joining the network is:")
        self.print_finger_table()

        # Start a threaded TCP server for the node to listen for incoming messages
        self.server = socketserver.ThreadingTCPServer(
            self.listen_address, lambda request, client_address, server: MessageHandler(
                request, client_address, server, self
            )
        )
        self.server.node = self
        server_thread = threading.Thread(target=self.server.serve_forever)
        server_thread.daemon = False
        print(f"Thread spawned for server. The listening address for this node is: {self.listen_address}")
        server_thread.start()

        self.join(join_through)

    @property
    def successor(self) -> int:
        """
        Get the successor for the current node, according to the local finger table

        :return: The current node's successor node number
        """
        return self.finger[1].node

    @successor.setter
    def successor(self, new_id: int) -> None:
        """
        Update the successor for the current node in the local finger table

        :param new_id: The id number which the current node's successor should be set to
        :return: None
        """
        self.finger[1].node = new_id

    @property
    def predecessor(self) -> int:
        """
        Get the predecessor for the current node, according to the local finger table

        :return: The current node's predecessor node number
        """
        return self._predecessor

    @predecessor.setter
    def predecessor(self, new_id: int) -> None:
        """
        Update the predecessor for the current node in the local finger table

        :param new_id: The id number which the current node's predecessor should be set to
        :return: None
        """
        self._predecessor = new_id

    @staticmethod
    def generate_node_id(port) -> int:
        """
        Generates a unique ID for the node, based on the port number it's listening on

        :param port: The port number on which the node is listening
        :return: The unique ID for the node
        """
        return port % NODES

    @staticmethod
    def lookup_node(n: int) -> tuple:
        """
        Creates a listening address to which RPC calls can be sent
        :param n: The ID of the node
        :return: Tuple of the form (hostname, port)
        """
        return 'localhost', int(BASE_PORT + n)  # ok for testing with small M

    @staticmethod
    def hash(data: Union[str, int]) -> str:
        return hashlib.sha1(data.encode('utf-8')).hexdigest()

    @staticmethod
    def find_hash_index(value: str) -> int:
        return int(value, 16) % NODES

    # def lookup_node(self, n):
    #     """
    #     Creates a listening address to which RPC calls can be sent
    #     :param n: The ID of the node
    #     :return: Tuple of the form (hostname, port)
    #     """
    #     if self.node_map is None:
    #         nm = {}
    #         for host in POSSIBLE_HOSTS:
    #             for port in POSSIBLE_PORTS:
    #                 addr = (host, int(port))
    #                 n = self.hash(host + str(port))
    #                 if n in nm:
    #                     print('cannot use', addr, 'hash conflict', n)
    #                 else:
    #                     nm[n] = addr
    #         self.node_map = nm
    #     # lookup in precomputed table
    #     print("Value of n is: ", n)
    #     return self.node_map.get(n, None)

    def print_finger_table(self) -> None:
        """
        Helper function to pretty-print the node's local finger table

        :return: None
        """
        print('-------------------------')
        print('start - range - node')
        for i in range(1, M + 1):
            print('{}    : ({}, {}) :  {}'.format(self.finger[i].start, self.finger[i].interval.start,
                                                  self.finger[i].interval.stop, self.finger[i].node))
        print('-------------------------')
        print('predecessor: {}'.format(self.predecessor))
        print('successor: {}'.format(self.successor))
        print('-------------------------')

    def print_bucket_keys(self) -> None:
        """
        Helper function to pretty-print the current node's bucket data

        :return: None
        """
        print(f"Printing the bucket data (keys) for node: {self.node_id}")
        print("Bucket keys = {")
        i = 0
        for key in self.bucket_data.keys():
            print(key)
            i += 1
            if i == 10:
                print("...")
                break
        print(f"Total number keys in the bucket: {len(self.bucket_data.keys())}")
        print("----------------------")

    def join(self, n_prime: int = None) -> None:
        """
        Function responsible for performing the initial JOIN for a new node in the network.

        :param n_prime: The ID of the node through which the current node has joined
        :return: None
        """
        if n_prime is not None:
            print(f"Executing join at Node: {self.node_id} through: {n_prime}")

            self.init_finger_table(n_prime)
            print(f"\nFinger table for node {self.node_id} after init_table is:")
            self.print_finger_table()

            self.update_others()
            print(f"\nFinger table for node {self.node_id} after update_others is:")
            self.print_finger_table()
        else:
            print(f"Node: {self.node_id} is the only node. Here is the new finger table:")
            for i in range(1, M+1):
                self.finger[i].node = self.node_id
            self.predecessor = self.node_id
            self.print_finger_table()

    def init_finger_table(self, n_prime: int) -> None:
        """
        Function responsible for initializing the current node's finger table

        :param n_prime: The ID of the node through which the current node has joined
        :return: None
        """
        print(f"Executing init_finger_table at Node: {self.node_id} for n_prime: {n_prime}")
        result = self.make_rpc_call(n_prime, "FIND_SUCCESSOR", self.finger[1].start)
        self.successor = result
        print(f"Finished executing FIND_SUCCESSOR at Node: {self.node_id} for: {n_prime}")

        result = self.make_rpc_call(self.successor, "GET_PREDECESSOR")
        self.predecessor = result
        print(f"Finished executing GET_PREDECESSOR for Node: {self.successor}")

        self.make_rpc_call(self.successor, "SET_PREDECESSOR", self.node_id)
        print(f"Finished executing SET_PREDECESSOR for Node: {self.successor}")

        for i in range(1, M):
            modrange = ModRange(self.node_id, self.finger[i].node, NODES)
            if modrange.__contains__(self.finger[i+1].start):
                self.finger[i+1].node = self.finger[i].node
            else:
                result = self.make_rpc_call(n_prime, "FIND_SUCCESSOR", self.finger[i+1].start)
                self.finger[i+1].node = result
                print(f"Finished setting finger[{i+1}].node to: {result} for node:{self.node_id}")

    def find_successor(self, id_: int) -> int:
        """
        Function responsible for finding the successor of the current (new) node in the network.
        It talks to other nodes in the network to accomplish this.

        :param id_: ID for which the successor has to be found
        :return: ID number of the respective successor node
        """
        print(f"Executing find_successor at Node: {self.node_id} for id: {id_}")
        n_prime = self.find_predecessor(id_)
        if n_prime == self.node_id:
            return self.successor
        return self.make_rpc_call(n_prime, "GET_SUCCESSOR")

    def find_predecessor(self, id_: int) -> int:
        """
        Function responsible for finding the predecessor of the current (new) node in the network.
        It talks to other nodes in the network to accomplish this.

        :param id_: ID for which the predecessor has to be found
        :return: ID number of the respective predecessor node
        """
        print(f"Executing find_predecessor at Node: {self.node_id} for id: {id_}")
        n_prime = int(self.node_id)
        if n_prime == self.node_id:
            n_prime_successor = self.successor
        else:
            n_prime_successor = self.make_rpc_call(n_prime, "GET_SUCCESSOR")
        modrange = ModRange(n_prime+1, (n_prime_successor % NODES)+1, NODES)
        while not (modrange.__contains__(id_)):
            data = self.make_rpc_call(n_prime, "CLOSEST_PRECEDING_FINGER", id_)
            n_prime = data
            if n_prime == self.node_id:
                n_prime_successor = self.successor
            else:
                n_prime_successor = self.make_rpc_call(n_prime, "GET_SUCCESSOR")
            modrange = ModRange(n_prime + 1, n_prime_successor + 1, NODES)
        print(f"Finished executing find_predecessor at Node: {self.node_id}, returning: {n_prime}")
        return n_prime

    def closest_preceding_finger(self, id_: int) -> int:
        """
        Given an ID, this function is responsible for finding the immediately preceding finger.

        :param id_: ID for which the closest preceding finger has to be found
        :return: ID number of the respective finger
        """
        print(f"Executing closest_preceding_finger at Node: {self.node_id} for id: {id_}")
        for i in range(M, 0, -1):
            modrange = ModRange(self.node_id+1, id_, NODES)
            if modrange.__contains__(self.finger[i].node):
                print(f"Finished executing CPF at Node: {self.node_id}, returning: {int(self.finger[i].node)}")
                return int(self.finger[i].node)
        print(f"Finished executing CPF at Node: {self.node_id}, returning: {int(self.node_id)}")
        return int(self.node_id)

    def update_others(self) -> None:
        """
        Function responsible for updating the finger tables of all the other known nodes

        :return: None
        """
        print(f"Executing update_others at Node: {self.node_id}")
        for i in range(1, M+1):
            node_copy = self.node_id
            p = self.find_predecessor((1 + self.node_id - 2**(i-1) + NODES) % NODES)
            self.node_id = node_copy
            self.make_rpc_call(p, "UPDATE_FINGER_TABLE", self.node_id, i)
        print("\nFinished updating all other tables")

    def update_finger_table(self, s: int, i: int) -> None:
        """
        Function responsible for updating the current node's finger table values

        :param s: The updated node value
        :param i: The index in the finger table whose node value has to be updated
        :return: None
        """
        print(f"Inside update_finger_table for node : {self.node_id}, and passing s,i as : {s, i}")
        modrange = ModRange(self.finger[i].start, self.finger[i].node, NODES)
        if self.finger[i].start != self.finger[i].node and modrange.__contains__(s):
            self.finger[i].node = s
            p = self.predecessor
            self.make_rpc_call(p, "UPDATE_FINGER_TABLE", s, i)
        print(f"\nFinished updating {self.node_id}'s finger table")

    def make_rpc_call(self, server_node: int, call_type: str, *params: Union[int, object, tuple]) -> Union[int, None]:
        """
        This function is responsible for marshalling an RPC call and sending it out.
        It also listens for a response and returns the requested resource.

        :param server_node: The node ID which is being called by the RPC request
        :param call_type: The resource that the RPC is requesting
        :param params: Payload sent in the RPC call
        :return: The requested resource
        """
        uri = f"http://{self.lookup_node(server_node)[0]}:{str(self.lookup_node(server_node)[1])}"
        print(f"Node: {self.node_id} is making a {call_type} RPC call to {server_node} at {uri} with params: {params}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.lookup_node(server_node))
        sock.send(pickle.dumps((call_type, params)))
        data = pickle.loads(sock.recv(BUFF_SIZE))
        return data

    def handle_rpc_request(self, method_name: str, *args: tuple) -> Union[int, None, tuple]:
        """
        This function parses the received RPC call and calls the appropriate function.

        :param method_name: The name of the action that the RPC requested
        :param args: Tuple of zero or more arguments passed in the RPC request
        :return: The requested resource
        """
        print(f"\n\nHere is the received RPC request: {method_name}, {args[0]}")
        payload = args[0]
        if method_name == "FIND_SUCCESSOR":
            data = self.find_successor(payload[0])
            print(f"\nFinger table at node {self.node_id} after receiving FS call is:")
            self.print_finger_table()
            return data
        elif method_name == "UPDATE_FINGER_TABLE":
            self.update_finger_table(payload[0], payload[1])
            print(f"\nFinger table at node {self.node_id} after recv UFT call is:")
            self.print_finger_table()
            return True
        elif method_name == "CLOSEST_PRECEDING_FINGER":
            return self.closest_preceding_finger(payload[0])
        elif method_name == "SET_SUCCESSOR":
            self.successor = payload[0]
            return True
        elif method_name == "SET_PREDECESSOR":
            self.predecessor = payload[0]
            return True
        elif method_name == "GET_SUCCESSOR":
            return self.successor
        elif method_name == "GET_PREDECESSOR":
            return self.predecessor
        elif method_name == "ROUTE_DATA":
            return self.route_data_population(payload[0], payload[1])
        elif method_name == "ADD_HASHED_DATA":
            return self.add_hashed_data(payload[0], payload[1])
        elif method_name == "QUERY":
            return self.route_query(payload[0])
        else:
            print("Received an invalid RPC request. Ignoring.")
            return None

    def add_hashed_data(self, key: str, data: object) -> None:
        """
        Given a key-value pair, append it into the current node's bucket

        :param key: 160-bit SHA-1 hash value
        :param data: JSON data to be stored in the bucket
        :return: None
        """
        print(f"The data for key: {key} belongs to {self.node_id}'s bucket. Adding it to the bucket.")
        self.bucket_data.update({key: data})
        print(f"The updated bucket for {self.node_id} is:")
        self.print_bucket_keys()

    def route_data_population(self, key: str, data: object) -> None:
        """
        This function is responsible for routing the 'populate node' request to the appropriate
        node. If the node's finger table doesn't have a range within which the query lies, it
        routes the request to the node halfway across the network.
        This routing happens in O(log n) time.

        :param key: 160-bit SHA-1 hash value
        :param data: JSON data to be stored in the bucket
        :return: None
        """
        print(f"Trying to route {key} to the right node")
        closest_node = self.find_hash_index(key)
        print(f"{key} % {NODES} = {closest_node}, so trying to find the node responsible for this bucket")
        if closest_node == self.node_id:
            self.add_hashed_data(key, data)
            return
        for i in range(1, M+1):
            if self.finger[i].interval.__contains__(closest_node):
                print(f"Hash modulo NODES = {closest_node} is being routed to {self.finger[i].node}")
                self.make_rpc_call(self.finger[i].node, "ADD_HASHED_DATA", key, data)
                return
        self.make_rpc_call(self.finger[M].node, "ROUTE_DATA", key, data)
        return

    def route_query(self, key: str) -> Union[int, None, tuple]:
        """
        This function is responsible for routing the 'query' request to the appropriate
        node. If the current node's bucket holds the query result, it returns that resource.
        If the node's finger table doesn't have a range within which the query lies, it
        routes the request to the node halfway across the network.
        This routing happens in O(log n) time.

        :param key: 160-bit SHA-1 hash value
        :return: a tuple containing the key-value pair associated with the input key
        """
        print(f"Trying to route query {key} to the right node")
        # if not self.bucket_data:
        #     return
        closest_node = self.find_hash_index(key)

        modrange = ModRange((self.predecessor+1%NODES), (self.node_id+1)%NODES, NODES)
        if modrange.__contains__(closest_node):
        # if self.predecessor < closest_node <= self.node_id:
            if key in self.bucket_data.keys():
                print(f"The key: {key} belongs to this bucket, at node: {self.node_id}")
                print(f"Returning the data associated with this key")
                return self.node_id, self.bucket_data[key]
            else:
                s1 = f"The key: {key} belongs to this bucket, at node: {self.node_id}"
                s2 = f"However, this key hasn't been added to the bucket yet. Kindly run the populate script first."
                return s1, s2

        for i in range(1, M+1):
            if self.finger[i].interval.__contains__(closest_node):
                print(f"Query is being routed from {self.node_id} to {self.finger[i].node}")
                return self.make_rpc_call(self.finger[i].node, "QUERY", key)
        print(f"Query is being routed from {self.node_id} to {self.finger[M].node}")
        return self.make_rpc_call(self.finger[M].node, "QUERY", key)


class MessageHandler(socketserver.BaseRequestHandler):
    """
    Handles incoming RPC calls received by the Node from other nodes in the distributed system.
    Also, it's responsible for sending the result back to the requester node via an RPC call.
    """

    def __init__(self, request, client_address, server, node_instance):
        """
        Initializes a new MessageHandler instance with the provided parameters.

        Args:
            request (socket.socket): The socket connection request from a client node.
            client_address (tuple): The address of the client node (hostname, port).
            server (socketserver.BaseServer): The server instance handling the connection.
            node_instance (Node): The Node instance associated with this MessageHandler.
        """
        self.node = node_instance
        super().__init__(request, client_address, server)
        self.node = node_instance

    def handle(self):
        """
        Overrides the handle() function. This class runs on the TCPThreadingServer thread.
        It's always running, listening for incoming messages, and handles them accordingly
        """

        data = None
        while not data:
            data = pickle.loads(self.request.recv(BUFF_SIZE))
        method_name = data[0]
        payload = data[1]
        result = self.node.handle_rpc_request(method_name, payload)
        self.request.send(pickle.dumps(result))


if __name__ == "__main__":
    """
    This program assumes the (BASE_PORT + n) way to lookup nodes. Enter the node's ID number.
    You are responsible for choosing an unused node ID number, and making sure that the node
    ID lies in the range [0, 2^M).

    Usage for 1st node: python chord_node.py NODE_ID
    Usage for subsequent nodes: python chord_node.py NODE_ID N_PRIME_NODE_ID

    :param sys.argv (list): Command-line arguments provided when running the script.
    :raises SystemExit: Exits the program if the correct number of arguments is not provided.
    """
    if len(sys.argv) == 2:
        node = ChordNode(int(sys.argv[1]))
    elif len(sys.argv) == 3:
        node = ChordNode(int(sys.argv[1]), int(sys.argv[2]))
    else:
        print("Incorrect startup command and/or arguments:")
        print("Usage for 1st node: python chord_node.py NODE_ID")
        print("Usage for subsequent nodes: python chord_node.py NODE_ID N_PRIME_NODE_ID")
        exit(1)
