import os
import time
import random

from logEntry import LogEntry

NodeList = {1: '127.0.0.1:50051', 2: '127.0.0.1:50052', 3: '127.0.0.1:50053', 4: '127.0.0.1:50054'}


class Node:
    isLeader = False
    nodeId = -1
    ipAddr = ""
    port = ""
    currentTerm = 0
    votedFor = None
    log = []
    commitLength = 0
    currentRole = "Follower"
    currentLeader = None
    votesReceived = []
    sentLength = {}
    ackedLength = {}
    lastTerm = 0
    leaderId = -1
    startTime = 0
    lastIndex = 0
    timer = 0
    val = False
    data = {}
    leaseDuration = 7
    leaseStartTime = 0
    timerOn = False
    leaseAcquired = False

    def __init__(self, nodeId, ip, port):
        self.nodeId = nodeId
        self.ipAddr = ip
        self.port = port
        self.timer = random.uniform(5, 11)
        self.log = []

        for i in range(1,10):
            self.sentLength[i] = 0;


        if os.path.isdir(f"logs_node_{nodeId}"):
            # take data from log files
            path = os.getcwd() + f"/logs_node_{nodeId}/"
            self.f = open(path + f"logs.txt", "a+")
            self.f1 = open(path + "metadata.txt", "a+")
            self.f2 = open(path + "dump.txt", "a+")
            # For now


        else:
            os.mkdir(f"logs_node_{nodeId}", 0o777)
            path = os.getcwd() + f"/logs_node_{nodeId}/"
            self.f = open(path + f"logs.txt", "a+")
            self.f1 = open(path + "metadata.txt", "a+")
            self.f2 = open(path + "dump.txt", "w+")

    def onCrashRecovery(self):
        self.currentRole = "Follower"
        self.currentLeader = None
        self.votesReceived = []
        self.sentLength = {}
        self.ackedLength = {}
        
        for i in range(1,10):
            self.sentLength[i] = 0;

    def onElectionTimeout(self):
        self.currentTerm += 1
        self.currentRole = "Candidate"
        self.votedFor = self.nodeId
        self.votesReceived = [self.nodeId]

    def startTimer(self):
        self.startTime = time.time()
        self.timerOn = True
    
    def cancelTimer(self):
        self.timerOn = False

    def checkTimeout(self):
        while True:
            if self.cancel() or self.isLeader:
                self.val=0
                return False
            if time.time() > self.startTime + self.timer:
                return True

    def cancel(self):
        return self.val
        #
        # if(len(self.log) > 0):
        #   self.lastTerm = self.log[len(self.log)-1].term
    def renew(self):
        self.val=1

    def acquireLease(self):
        self.leaseStartTime = time.time()
        self.leaseAcquired = True


    def checkLeaseExpiry(self):

        if(time.time() >= self.leaseStartTime + self.leaseDuration):
            self.leaseAcquired = False
            return True # True Means expired
        else:
            return False

    def writelog(self):
        for i in self.log:
            if i.key=="NO-OP":
                self.f.write(i.key+"\n")
            else:
                self.f.write(f"{i.key} {i.value} {i.term} \n")





