import grpc
import sys
import random
import time
import threading
from concurrent import futures
import raft_pb2_grpc
from raft_pb2 import RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse

class RaftNode(raft_pb2_grpc.RaftServicer):
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.term = 0
        self.voted_for = None
        self.state = 'follower'
        self.current_leader = None
        self.votes_received = []
        self.peers = peers
        self.log = []
        self.election_timeout = random.randint(5, 10)
        self.current_timeout = time.time()
        

    def recovery_from_crash(self):
        self.state = 'follower'
        self.current_leader = None

    # suspects leader has failed, or on election timout
    def start_election(self):
        self.term += 1
        self.state = 'candidate'
        self.voted_for = self.node_id
        self.votes_received = []
        self.votes_received.append(self.node_id)  # Vote for itself
        lastTerm = 0

        if len(self.log) > 0:
            lastTerm = self.log[len(self.log) - 1].term

        # Request votes from peers
        for peer in self.peers:
            self.send_request_vote(peer, lastTerm)

    def reset_timeout(self):
        self.current_timeout = time.time()

    def on_timeout(self):
        if self.state == 'follower':
            self.state = 'candidate'
            self.start_election()

    def send_heartbeats(self):
        i = 1
        while self.state == 'leader':
            for peer in self.peers:
                self.send_append_entries(peer)
            time.sleep(0.5)  # Send heartbeats every second
            print(i)
            i += 1
            if i == 10:
                time.sleep(10)

    def send_request_vote(self, peer, lastTerm):
        print("SEND REQUEST VOTE")
        request = RequestVoteRequest(
            term=self.term,
            candidate_id=self.node_id,
            last_log_index=len(self.log),
            last_log_term=lastTerm,
        )
        response_list = []
        for each_peer in self.peers:
            channel = grpc.insecure_channel(each_peer)
            stub = raft_pb2_grpc.RaftStub(channel)
            response = stub.RequestVote(request)
            response_list.append(response)

        if self.state == 'candidate': 
            for response in response_list:
                if response.vote_granted and self.term == response.term:
                    self.votes_received.append(response.nodeId)
                    if len(self.votes_received) > abs((len(self.peers) + 1) / 2):
                        print(len(self.votes_received))
                        print(self.votes_received)
                        print(f"Node {self.node_id} became the leader {self.term}")
                        self.state = 'leader'
                        self.current_leader = self.node_id
                        self.send_heartbeats()
                        # For each follower send length and acklength

                    elif response.term > self.term:
                        self.term = response.term
                        self.state = 'follower'
                        self.voted_for = None


    def send_append_entries(self, peer):
        print(f"SEND APPEND ENTRIES - {self.state}")
        request = AppendEntriesRequest(
            term=self.term,
            leader_id=self.node_id,
            prev_log_index=len(self.log) - 1,
            prev_log_term=self.term,
            entries=[],
            leader_commit=0,
        )
        channel = grpc.insecure_channel(peer)
        stub = raft_pb2_grpc.RaftStub(channel)
        stub.AppendEntries(request)


    def RequestVote(self, request, context):
        print("REQUEST VOTE")
        if request.term > self.term:  
            self.term = request.term
            self.state = 'follower'
            self.voted_for = None
        lastTerm = 0
        if len(self.log) > 0:
            lastTerm = self.log[len(self.log) - 1].term

        logOk = (request.term > lastTerm) or (request.term == lastTerm and len(self.log) <= request.last_log_index)  
        if request.term == self.term and logOk and self.voted_for is None or self.voted_for == request.candidate_id:
            self.voted_for = request.candidate_id
            return RequestVoteResponse(term=self.term, nodeId=self.node_id, vote_granted=True)
        return RequestVoteResponse(term=self.term, nodeId=self.node_id, vote_granted=False)

    def AppendEntries(self, request, context):
        if request.term >= self.term:
            print(f"APPEND ENTRIES - {self.state}")
            self.term = request.term
            self.state = 'follower'
            self.reset_timeout()
            self.election_timeout = random.randint(5, 10)
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
        # current timeout scale miliseconds
        while True:          
            if raft_node.state == 'follower' and (time.time()  - raft_node.current_timeout) > raft_node.election_timeout:
                print((time.time()  - raft_node.current_timeout), raft_node.election_timeout)
                print(f"Node {node_id} starting election due to timeout.")
                raft_node.on_timeout()
                
            
    # Start the election timer in a separate thread
    threading.Thread(target=election_timer, daemon=True).start()

    # Keep the main thread running
    server_thread.join() 
    

if __name__ == "__main__":
    main()
