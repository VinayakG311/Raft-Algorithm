import os
import random
import socket
import sys
import threading
import time
from concurrent import futures
from logEntry import LogEntry

import grpc

import raft_pb2
import raft_pb2_grpc
from raftNode import Node, NodeList

port = sys.argv[1]
nodeId = sys.argv[2]
ip = socket.gethostbyname(socket.gethostname())

leader = False

node: Node = Node(nodeId=int(nodeId), ip=ip, port=port)
dumper = open(f"logs_node_{nodeId}/dump.txt","a")
open_nodes = {1:'127.0.0.1:50051', 2:'127.0.0.1:50052',3:'127.0.0.1:50053', 4:'127.0.0.1:50054'}
# all_ip = {'127.0.0.1:50051': 1, '127.0.0.1:50052': 2, '127.0.0.1:50053': 3, '127.0.0.1:50054': 4}


def reWrite():
    node_ip = ip + ":" + port
    node_id = nodeId
    f = open("nodes.txt", "r")
    lines = f.readlines()
    lines = [line for line in lines if line.strip() != f"{node_ip} {node_id}"]
    f.close()
    f = open("nodes.txt", "w")
    f.writelines(lines)

    sys.exit(0)


# def NodeDetector():
#     try:
#         # time.sleep(4)

#         while True:
#             ip_exist = []
#             f = open("nodes.txt", "r")
#             nodes = f.read().split("\n")
#             f.close()
#             for i in nodes:
#                 k = i.split(" ")
#                 ip_exist.append(k[0])
#                 if k == ['']:
#                     continue
#                 if int(k[1]) not in open_nodes:
#                     open_nodes[int(k[1])] = k[0]
#                     print(open_nodes)
#             for k, v in all_ip.items():
#                 if k not in ip_exist and v in open_nodes.keys():
#                     del open_nodes[v]

#     except KeyboardInterrupt:
#         reWrite()
#         return


def checkLease():
    while (True):
        if node.leaseAcquired and node.checkLeaseExpiry():
            if node.leaderId not in open_nodes:
                print("Leader")
                node.currentRole = "Leader"
                node.currentLeader = node.nodeId
                node.isLeader = True
                node.leaderId = node.nodeId

                entry = LogEntry(node.lastTerm, node.lastIndex + 1, "NO-OP", "")
                node.log.append(entry)

                for j, i in open_nodes.items():
                    if i == node.ipAddr + ":"+node.port:
                        continue

                    # Replicating logs

                    node.sentLength[j] = len(node.log)
                    node.ackedLength[j] = 0
                    req1 = [node.nodeId, open_nodes[node.nodeId], j, i]
                    # print(req1)
                    # SendBroadcast(entry)
                    ReplicateLogs(req1, False)


def setValue(key, value):
    # readItr = open("data.txt", "r")
    writeItr = open("data.txt", "a")
    print("FILE OPENED")
    # entries = readItr.readlines()

    writeItr.write(key + " " + value + "\n")

    writeItr.close()
    # readItr.close()
    return -1


def getValue(key):
    readItr = open("data.txt", "r")
    entries = readItr.readlines()
    for entry in entries:
        if entry.split(" ")[0] == key:
            readItr.close()
            return entry.split(" ")[1]
    readItr.close()
    return "NO"


def noOp():
    writeItr = open("data.txt", "a")
    writeItr.write("No-Op+\n")
    writeItr.close()
    return "NO"


def metadatawriter():
    while True:
        time.sleep(1)
        f = open(f"logs_node_{nodeId}/metadata.txt", "w")
        f.write(f"commitLength: {node.commitLength}\nTerm: {node.currentTerm}\nvotedFor: {node.votedFor}")
        f.close()

def ReplicateLogs(req, heartbeat):
    node.acquireLease()
    prefix = node.sentLength[req[2]]
    suffix = node.log[prefix:]
    for i in range(len(suffix)):
        entry = suffix[i]

        suffix[i] = raft_pb2.entry(index=entry.index, term=entry.term, key=entry.key, val=entry.value)

    prefixTerm = 0
    if prefix > 0:
        prefixTerm = node.log[prefix - 1].term

    # request = raft_pb2.AppendEntriesArgs()
    dumper.write(f"Leader {node.currentLeader} sending heartbeat & Renewing Lease \n")
    try:
        with grpc.insecure_channel(req[3]) as channel:
            stub = raft_pb2_grpc.RaftStub(channel)
            request = raft_pb2.ReplicateLogRequestArgs(leaderId=node.nodeId, currentTerm=node.currentTerm,
                                                    prefixLen=prefix, prefixTerm=prefixTerm,
                                                    commitLength=node.commitLength, suffix=suffix, heartBeat=heartbeat)
            res = stub.ReplicateLogRequest(request)

            # print(res)
    except:
        dumper.write(f"Error occurred while sending RPC to Node {req[3]}")


def sendHeartbeat():
    try:
        while True:

            time.sleep(1)
            if node.isLeader:

                for j, i in open_nodes.items():
                    if i == node.ipAddr + ":" + node.port:
                        continue
                    req1 = [node.nodeId, open_nodes[node.nodeId], j, i]

                    ReplicateLogs(req1, True)

    except KeyboardInterrupt:
        reWrite()
        return


def timeout():
    time_rand = time.time() + random.uniform(1, 2)
    while True:

        if time.time() >= time_rand:
            return True


def StartElection():
    node.startTimer()
    # print("TRYING ELECTIONS ..")
    # node.acquireLease()
    print(node.timer)
    if ((node.checkTimeout())):
        dumper.write(f"Node {node.nodeId} election timer timed out, Starting election. \n")
        longestLease = 0
        leaseStart = 0

        node.currentTerm += 1
        node.currentRole = "Candidate"
        node.votedFor = node.nodeId
        node.votesReceived = [node.nodeId]
        node.lastTerm = 0
        if len(node.log) > 0:
            node.lastTerm = node.log[len(node.log) - 1].term
        node.startTimer()

        print("SENDING VOTE REQUESTS")
        for j, i in open_nodes.items():
            if i == node.ipAddr + ":" + node.port:
                continue
            try:
                with grpc.insecure_channel(i) as channel:
                    stub = raft_pb2_grpc.RaftStub(channel)
                    request = raft_pb2.RequestVotesArgs(term=node.currentTerm, candidateId=node.nodeId,
                                                        lastLogTerm=node.lastTerm,
                                                        lastLogIndex=node.lastIndex)
                    response = stub.RequestVote(request)

                    if response.longestDurationRem > longestLease:
                        longestLease = response.longestDurationRem
                        leaseStart = time.time()

                    if response.voteGranted == True and node.currentRole == "Candidate" and node.currentTerm == response.term:
                        node.votesReceived.append(response.NodeId)
            except:
                dumper.write(f"Error occurred while sending RPC to Node {j}")
        print(node.votesReceived)
        if len(node.votesReceived) >= len(open_nodes) / 2:
            if time.time() < leaseStart + longestLease:
                dumper.write("New Leader waiting for Old Leader Lease to timeout")
            while time.time() < leaseStart + longestLease:

                time.sleep(0.5)
            node.acquireLease()

            dumper.write(f"Node {node.nodeId} became the leader for term {node.currentTerm} \n")
            print(node.votesReceived)
            print("Leader")
            node.currentRole = "Leader"
            node.currentLeader = node.nodeId
            node.isLeader = True
            node.leaderId = node.nodeId
            entry = LogEntry(node.lastTerm, node.lastIndex + 1, "NO-OP", "")
            node.log.append(entry)

            for j, i in open_nodes.items():

                if i == node.ipAddr +":"+node.port:

                    continue

                # Replicating logs

                node.sentLength[j] = 0
                node.ackedLength[j] = 0
                req1 = [node.nodeId, open_nodes[node.nodeId], j, i]
                print(req1)
                # SendBroadcast(entry)

                ReplicateLogs(req1, False)

        else:
            print("Follower")
            if response.term >= node.currentTerm:
                dumper.write(f"{node.nodeId} Stepping down \n")
                node.currentTerm = response.term
                node.currentRole = "Follower"
                node.votedFor = None
                node.cancelTimer()
        # for k, l in open_nodes.items():
        #     if l == node.ipAddr + ":" + node.port:
        #         continue
        #     try:
        #         with grpc.insecure_channel(l) as channel:
        #             stub = raft_pb2_grpc.RaftStub(channel)
        #             request = raft_pb2.RequestVotesArgs(term=node.currentTerm, candidateId=node.nodeId,
        #                                                 lastLogTerm=node.lastTerm,
        #                                                 lastLogIndex=node.lastIndex)
        #             response = stub.RequestVote(request)
        #
        #             if response.longestDurationRem > longestLease:
        #                 longestLease = response.longestDurationRem
        #                 leaseStart = time.time()
        #
        #             if response.voteGranted == True and node.currentRole == "Candidate" and node.currentTerm == response.term:
        #                 node.votesReceived.append(response.NodeId)
        #                 print(node.votesReceived)
        #                 if len(node.votesReceived) >= (len(open_nodes)+1) / 2:
        #
        #                     while time.time() < leaseStart + longestLease:
        #                         time.sleep(0.5)
        #                     node.acquireLease()
        #
        #                     dumper.write(f"Node {node.nodeId} became the leader for term {node.currentTerm}")
        #                     print(node.votesReceived)
        #                     print("Leader")
        #                     node.currentRole = "Leader"
        #                     node.currentLeader = node.nodeId
        #                     node.isLeader = True
        #                     node.leaderId = node.nodeId
        #                     entry = LogEntry(node.lastTerm, node.lastIndex + 1, "NO-OP", "")
        #                     node.log.append(entry)
        #
        #                     for j, i in open_nodes.items():
        #
        #                         if i == node.ipAddr +":"+node.port:
        #
        #                             continue
        #
        #                         # Replicating logs
        #
        #                         node.sentLength[j] = len(node.log)
        #                         node.ackedLength[j] = 0
        #                         req1 = [node.nodeId, open_nodes[node.nodeId], j, i]
        #                         print(req1)
        #                         # SendBroadcast(entry)
        #
        #                         ReplicateLogs(req1, False)
        #                     return
        #
        #             elif(response.term >= node.currentTerm):
        #                 # print("Follower")
        #                 # if response.term >= node.currentTerm:
        #                 dumper.write(f"{node.nodeId} Stepping down")
        #                 node.currentTerm = response.term
        #                 node.currentRole = "Follower"
        #                 node.votedFor = None
        #                 node.cancelTimer()
        #                 return
        #     except Exception as e:
        #         print(f"ERROR NODE {k} {e}")
        #         pass


def SuspectFail():
    return False


class RaftServicer(raft_pb2_grpc.RaftServicer):

    def AppendEntries(self, request, context):
        if (request.heartBeat):
            print("HeartBeat Received")
        if len(request.suffix) > 0 and len(node.log) > request.prefixLen:
            index = min(len(node.log), request.prefixLen + len(request.suffix)) - 1
            if node.log[index].term != request.suffix[index - request.prefixLen].term:
                node.log = node.log[0:request.prefixLen]

        if request.prefixLen + len(request.suffix) > len(node.log):
            for i in range(len(node.log) - request.prefixLen, len(request.suffix)):
                re = LogEntry(term=request.suffix[i].term, value=request.suffix[i].val, key=request.suffix[i].key,
                              index=request.suffix[i].index)
                node.log.append(re)

        if request.leaderCommit > node.commitLength:
            for i in range(node.commitLength, request.leaderCommit):
                pass  # !TODO send back to application
            node.commitLength = request.leaderCommit
        print(node.log)
        path = os.getcwd() + f"/logs_node_{nodeId}/"
        f = open(path + f"logs.txt", "w")
        for i in node.log:
            data=""
            if i.key == "NO-OP":
                data = i.key
                f.write(i.key + "\n")
            else:
                data= f"SET {i.key} {i.value} {i.term}"
                f.write(f"SET {i.key} {i.value} {i.term} \n")
            if node.nodeId==node.currentLeader:
                dumper.write(f"Node {node.nodeId} (leader) committed the entry {data} to the state machine.")
            else:
                dumper.write(f"Node {node.nodeId} (follower) committed the entry {data} to the state machine.")
        # return super().AppendEntries(request, context)

    def RequestVote(self, request, context):
        print(f"VOTE REQ BY {request.candidateId}")
        if request.term > node.currentTerm:
            node.currentTerm = request.term
            node.currentRole = "Follower"
            node.votedFor = None
        node.lastTerm = 0
        if len(node.log) > 0:
            node.lastTerm = node.log[len(node.log) - 1].term

        ok = (request.lastLogTerm > node.lastTerm) or (
                request.lastLogTerm == node.lastTerm and request.lastLogIndex >= len(node.log))

        vote = True
        print("before:",node.votedFor)
        if request.term == node.currentTerm and ok and (node.votedFor is None or node.votedFor == request.candidateId):
            node.votedFor = request.candidateId
            print("after:",node.votedFor)
            dumper.write(f"Vote granted for Node {node.votedFor} in term {node.currentTerm}")
            node.val = True
            # node.leaderId = request.candidateId
            print("VOTED ..")

        else:
            dumper.write(f"Vote denied for Node {node.votedFor} in term {node.currentTerm}")
            vote = False
        longestLease = 0
        if node.leaseStartTime > 0:
            longestLease = time.time() - node.leaseStartTime

        return raft_pb2.RequestVotesRes(term=node.currentTerm, voteGranted=vote, longestDurationRem=longestLease,
                                        NodeId=node.nodeId)

    def ServeClient(self, request, context):
        print(node.nodeId, node.currentLeader)
        req = request.Request.split(" ")
        # request = request.Request.split(" ")
        operation = req[0]
        print(operation)
        data = ""
        # # ! Pass the message to leader
        # # TODO
        if node.nodeId != node.currentLeader:
            print("Not leader ....")
            return raft_pb2.ServeClientReply(Data=str(data), LeaderID=str(node.currentLeader), Success=False)
        else:
            dumper.write(f"Node {node.currentLeader} (leader) received an {operation} request")
            if operation == "SET":
                key = req[1]
                value = req[2]
                node.data[key] = value

                entry = LogEntry(node.lastTerm, node.lastIndex + 1, key, value)
                node.log.append(entry)
                # SendBroadcast(entry)
                # ReplicateLogs()
            elif operation == "GET":

                key = req[1]
                for entry in node.log:
                    if entry.key == key:
                        data = entry.value
                # data = str(node.data[key])
                # print(node.data[key])
            else:

                data = noOp()
                entry = LogEntry(node.lastTerm, node.lastIndex + 1, "NO-OP", "")
                node.log.append(entry)
                # SendBroadcast(entry)
            for j, i in open_nodes.items():
                if i == node.ipAddr +":"+node.port:
                    continue

                # Replicating logs

                req1 = [node.nodeId, open_nodes[node.nodeId], j, i]
                # print(req1)
                # SendBroadcast(entry)
                ReplicateLogs(req1, False)

        return raft_pb2.ServeClientReply(Data=str(data), LeaderID=str(node.currentLeader), Success=True)
        # print(request.request)
        # return super().ServeClient(request, context)

    def ReplicateLogRequest(self, request, context):
        # if(request.term > )
        # if request.heartbeat:
        #     node.renew()
        # TODO implement this functionality
        if request.currentTerm > node.currentTerm:
            node.currentTerm = request.currentTerm
            node.votedFor = None
            node.cancelTimer()
            # Cancel Election
        if request.currentTerm == node.currentTerm:
            node.currentRole = "Follower"
            node.currentLeader = request.leaderId
        ok = (len(node.log) >= request.prefixLen) or (
                request.prefixLen == 0 and node.log[request.prefixLen - 1].term == request.prefixTerm)
        if node.currentTerm == request.currentTerm and ok:
            # Append Entries
            node.acquireLease()
            req = raft_pb2.AppendEntriesArgs(term=node.currentTerm, leaderId=node.currentLeader,
                                             prevLogIndex=node.lastIndex, prevLogTerm=node.lastTerm,
                                             suffix=request.suffix, leaderCommit=request.commitLength, leaseInterval=0,
                                             prefixLen=request.prefixLen, heartBeat=request.heartBeat)
            res = self.AppendEntries(req, context)
            ack = request.prefixLen + len(request.suffix)
            dumper.write(f"Node {node.nodeId} accepted AppendEntries RPC from {node.currentLeader}")
            try:
                with grpc.insecure_channel(open_nodes[node.currentLeader]) as channel:
                    stub = raft_pb2_grpc.RaftStub(channel)

                    req = raft_pb2.ReplicateLogResponseArgs(followerId=node.nodeId, followerTerm=node.currentTerm, ack=ack,
                                                            success=True)
                    res = stub.ReplicateLogResponse(req)
            except:
                pass
                # print(res)
            # Send Ack to leader of success
        else:
            # Send Ack to leader of failure
            dumper.write(f"Node {node.nodeId} rejected AppendEntries RPC from {node.currentLeader}")
            try:
                with grpc.insecure_channel(open_nodes[node.leaderId]) as channel:
                    stub = raft_pb2_grpc.RaftStub(channel)
                    req = raft_pb2.ReplicateLogResponseArgs(followerId=node.nodeId, followerTerm=node.currentTerm, ack=0,
                                                            success=False)
                    res = stub.ReplicateLogResponse(req)
                return raft_pb2.ReplicateLogRequestRes(nodeId=node.nodeId, currentTerm=node.currentTerm, ackLen=0,
                                               receivedMessage=True)
            except:
                pass
        

    def ReplicateLogResponse(self, request, context):
        if node.currentTerm == request.followerTerm and node.currentRole == "Leader":
            if request.success == True and request.ack >= node.ackedLength[request.followerId]:
                node.sentLength[request.followerId] = request.ack
                node.ackedLength[request.followerId] = request.ack
                # Commit Log
                req = raft_pb2.CommitArgs()
                res = self.CommitEntries(req, context)
            elif node.sentLength[request.followerId] > 0:
                node.sentLength[request.followerId] -= 1
                # Replicate Log
        elif request.followerTerm > node.currentTerm:
            node.currentTerm = request.followerTerm
            node.currentRole = "Follower"
            node.votedFor = None
            # Cancel Election
        return raft_pb2.ReplicateLogResponseRes()

    def CommitEntries(self, request, context):
        minacks = (len(open_nodes) + 1) / 2
        ready = []
        # TODO: Didnt get this
        for i in range(1, len(node.log)):
            ready.append(0)
        if ready != [] and max(ready) > node.commitLength and node.log[max(ready) - 1].term == node.currentTerm:
            for i in range(node.commitLength, max(ready)):
                # Deliver Log message to application
                continue
            node.commitLength = max(ready)

    def RefreshLease(self, request, context):
        if request.ack == 1:
            print(f"Lease Renewed by Leader: {node.currentLeader}")
            node.cancel()
        return raft_pb2.LeaseRes(ack=1)


def serve():
    try:
        global node
        n = -1
        addr = ip + ":" + port
        for k, v in NodeList.items():
            if v == addr:
                n = k
        print(nodeId, addr)
        f = open("nodes.txt", "a")
        f.write(ip + ":" + port + " " + str(nodeId) + " " + "\n")
        f.close()
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
        raft_pb2_grpc.add_RaftServicer_to_server(RaftServicer(), server)
        server.add_insecure_port(f"[::]:{port}")
        server.start()
        time.sleep(10)

        try:

            StartElection()

        except KeyboardInterrupt:
            reWrite()
            pass
        try:
            while True:
                time.sleep(3600)  # One hour
        except KeyboardInterrupt:
            reWrite()
            server.stop(0)
    except KeyboardInterrupt:
        reWrite()


t = []
if __name__ == '__main__':
    with open(f"logs_node_{node.nodeId}/logs.txt", "r") as f:
        lines = f.readlines()
        if (len(lines) > 0):
            node.onCrashRecovery()
    th1 = threading.Thread(target=serve)
    th2 = threading.Thread(target=checkLease)
    # th3 = threading.Thread(target=NodeDetector)
    th4 = threading.Thread(target=sendHeartbeat)
    th5 = threading.Thread(target=metadatawriter)
    t.append(th1)
    t.append(th2)
    # t.append(th3)
    t.append(th4)
    t.append(th5)

    try:
        for i in t:
            i.start()
        for i in t:
            i.join()

    except KeyboardInterrupt:
        if node.isLeader:
            dumper.write(f"Leader {node.currentLeader} lease renewal failed. Stepping Down")
        dumper.close()
        reWrite()


