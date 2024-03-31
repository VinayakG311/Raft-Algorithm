import jsonpickle

from raftNode import Node

node = Node(ip="1",nodeId="2",port="3")

f= open("dump.txt","w")
data = jsonpickle.encode(node)

f.write(data)