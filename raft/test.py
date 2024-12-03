import grpc
import sys
import random
import time
import threading
from concurrent import futures
from concurrent.futures import TimeoutError
import raft_pb2_grpc
from raft_pb2 import RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse

class RaftNode(raft_pb2_grpc.RaftServicer):
    def __init__(self, node_id, peers, election_timeout_range=(1.5, 3), heartbeat_interval=0.01):
        self.node_id = node_id
        self.term = 0
        self.voted_for = None
        self.state = 'follower'
        self.current_leader = None
        self.votes_received = []
        self.peers = peers
        self.log = []
        self.election_timeout = random.uniform(*election_timeout_range)
        self.current_timeout = time.time()
        self.heartbeat_interval = heartbeat_interval
        self.lock = threading.Lock()  # Lock for thread safety

    def recovery_from_crash(self):
        self.state = 'follower'
        self.current_leader = None

    def start_election(self):
        print("START ELECTION")
        self.term += 1
        self.state = 'candidate'
        self.voted_for = self.node_id
        self.votes_received = [self.node_id]  # Vote for itself
        lastTerm = self.log[-1].term if self.log else 0

        # Request votes from peers
        peers = update_peers(self)
        for peer in self.peers:
            self.send_request_vote(peer, lastTerm, peers)

        

    def reset_timeout(self):
        self.current_timeout = time.time()

    def on_timeout(self):
        with self.lock:
            while True:
                if self.state == 'follower':
                    print(f"Node {self.node_id} timed out. Starting election.")
                    self.start_election()

    def send_heartbeats(self):
        i = 1
        while self.state == 'leader':
            for peer in self.peers:
                self.send_append_entries(peer)
            time.sleep(self.heartbeat_interval)


    def send_request_vote(self, peer, lastTerm, peers):
        print("SEND REQUEST VOTE")
        request = RequestVoteRequest(
            term=self.term,
            candidate_id=self.node_id,
            last_log_index=len(self.log),
            last_log_term=lastTerm,
        )
        
        with grpc.insecure_channel(peer) as channel:
            stub = raft_pb2_grpc.RaftStub(channel)
            try:
                response = stub.RequestVote(request, timeout=0.5)
                self.handle_vote_response(response, peers)
            except grpc.RpcError as e:
                print(f"Error contacting peer {peer}: {e}")
                None
        


    def handle_vote_response(self, response, peers):
        if response.vote_granted and self.term == response.term:
            self.votes_received.append(response.nodeId)
            if len(self.votes_received) > (len(peers) // 2):
                print(f"Node {self.node_id} became leader for term {self.term}")
                self.state = 'leader'
                self.current_leader = self.node_id
                threading.Thread(target=self.send_heartbeats, daemon=True).start()
        elif response.term > self.term:
            self.term = response.term
            self.state = 'follower'
            self.voted_for = None

    def send_append_entries(self, peer):
        print(f"Sending AppendEntries to {peer}")
        request = AppendEntriesRequest(
            term=self.term,
            leader_id=self.node_id,
            prev_log_index=len(self.log) - 1,
            prev_log_term=self.term,
            entries=[],  # Add entries here if necessary
            leader_commit=0,
        )
        with grpc.insecure_channel(peer) as channel:
            stub = raft_pb2_grpc.RaftStub(channel)
            try:
                stub.AppendEntries(request, timeout=0.5)
            except grpc.RpcError as e:
                # print(f"Error sending append entries to {peer}: {e}")
                None

    def RequestVote(self, request, context):
        print(f"Received RequestVote from {request.candidate_id}")
        # with self.lock:
        if request.term > self.term:
            self.reset_timeout()
            self.term = request.term
            self.state = 'follower'
            self.voted_for = None

        lastTerm = self.log[-1].term if self.log else 0
        logOk = (request.term > lastTerm) or (request.term == lastTerm and len(self.log) <= request.last_log_index)

        if (request.term == self.term and logOk and 
            (self.voted_for is None or self.voted_for == request.candidate_id)):
            self.voted_for = request.candidate_id
            return RequestVoteResponse(term=self.term, nodeId=self.node_id, vote_granted=True)
        return RequestVoteResponse(term=self.term, nodeId=self.node_id, vote_granted=False)

    def AppendEntries(self, request, context):
        if request.term >= self.term:
            print("Received AppendEntries")
            with self.lock:
                self.term = request.term
                self.state = 'follower'
                self.reset_timeout()
                self.election_timeout = random.uniform(1.5, 3) 
            return AppendEntriesResponse(term=self.term, success=True)
        return AppendEntriesResponse(term=self.term, success=False)

def wait_for_peers(peers, timeout=30):
    """Wait until all peers are reachable"""
    start_time = time.time()    
    while time.time() - start_time < timeout:
        all_connected = True
        for peer in peers:
            try:
                channel = grpc.insecure_channel(peer)
                grpc.channel_ready_future(channel).result(timeout=2)
            except grpc.FutureTimeoutError:
                all_connected = False
                print(f"Waiting for peer {peer} to become available...")
                time.sleep(1)
                break
        if all_connected:
            print("All peers are connected.")
            return True
    print("Timeout waiting for peers.")
    return False

def update_peers(raft_node):
    peers = []
    for peer in raft_node.peers:
        try:
            channel = grpc.insecure_channel(peer)
            grpc.channel_ready_future(channel).result(timeout=0.5)
            peers.append(peer)
        except grpc.FutureTimeoutError:
            None
    
    return peers

def run_server(raft_node):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    raft_pb2_grpc.add_RaftServicer_to_server(raft_node, server)
    server.add_insecure_port(f'[::]:{raft_node.node_id.split(":")[1]}')
    server.start()
    print(f"Server started for node {raft_node.node_id}")
    server.wait_for_termination()

def main():
    if len(sys.argv) < 3:
        print("Usage: python test.py <node_id> <peer1> <peer2> ... <peern>")
        sys.exit(1)

    node_id = sys.argv[1]
    peers = sys.argv[2:]

    raft_node = RaftNode(node_id, peers)

    # Start the gRPC server in a separate thread
    server_thread = threading.Thread(target=run_server, args=(raft_node,))
    server_thread.start()

    # Wait for peers to become available
    if not wait_for_peers(peers):
        print("Failed to connect to all peers. Exiting.")
        sys.exit(1)

    # Election timeout loop
    def election_timer():
        while True:
            if raft_node.state == 'follower' and (time.time() - raft_node.current_timeout) > raft_node.election_timeout:
                print(f"Node {node_id} starting election due to timeout.")
                raft_node.on_timeout()

    # Start the election timer in a separate thread
    threading.Thread(target=election_timer, daemon=True).start()

    # Keep the main thread running
    server_thread.join()

if __name__ == "__main__":
    main()