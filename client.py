import grpc

import raft_pb2
import raft_pb2_grpc
import sys

NodeList = {1: '127.0.0.1:50051', 2: '127.0.0.1:50052', 3: '127.0.0.1:50053', 4: '127.0.0.1:50054'}


def run():
    leader_addr = "127.0.0.1:50052"
    
    while True:
        # print(leader_addr)
        try:
            with grpc.insecure_channel(leader_addr) as channel:
                stub = raft_pb2_grpc.RaftStub(channel)
                req = input("Enter Request: ")
                print(req.split())
                res = stub.ServeClient(raft_pb2.ServeClientArgs(Request=req))
                print(res)
                if not res.Success:
                    print(f"Leader is {res.LeaderID}")
                    leader_addr = NodeList[int(res.LeaderID)]
        except:
            print("NO LEADER CURRENTLY")
            sys.exit(0)



if __name__ == "__main__":
    run()
