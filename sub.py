#
#   Request-reply client in Python
#   Connects REQ socket to tcp://localhost:5559
#   Sends "Hello" to server, expects "World" back
#
import zmq
import os
import random
import threading
import time
import signal, sys
from middleware import register_sub
from middleware import connect
from middleware import wait_for_published_topic

import argparse   # argument parser

# Now import the kazoo package that supports Python binding
# to ZooKeeper
from kazoo.client import KazooClient

topic = ""

def signal_handler(sig, frame):
	print('You pressed Ctrl+Z!')
	topic = input("Enter the new topic: ")
	print("Your input: "+topic)


##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
	parser = argparse.ArgumentParser ()

	# add optional arguments
	parser.add_argument ("-a", "--zkIPAddr", default="127.0.0.1", help="ZooKeeper server ip address, default 127.0.0.1")
	parser.add_argument ("-p", "--zkPort", type=int, default=2181, help="ZooKeeper server port, default 2181")
	parser.add_argument ("-ppath", "--zkPpath", default="/broker", help="ZooKeeper Broker Parent Path")

    # add positional arguments in that order
	parser.add_argument ("mode", help="D: Direct pub/sub communication, I: Pub/Sub communication thru Broker")

	# command example : python3 sub.py I

    # parse the args
	args = parser.parse_args ()

	return args


class Sub_Driver ():

	# constructor
	def __init__ (self, args):
		self.zkIPAddr = args.zkIPAddr  # ZK server IP address
		self.zkPort = args.zkPort # ZK server port num
		self.ppath = args.zkPpath # refers to the parent znode path
		self.zk = None  # session handle to the server
		self.first_flag = True
		self.reconnect_flag = False

	# Initialize variables
	def init_sub (self):
		# It is here that we connect with the zookeeper server
		# and see if the condition has been met
		try:
			# Instantiate zookeeper client object
			# right now only one host; it could be the ensemble
			hosts = self.zkIPAddr + str (":") + str (self.zkPort)
			self.zk = KazooClient (hosts)

		except:
			print("Unexpected error in ClientApp::run", sys.exc_info()[0])
			raise

	def run_sub(self):
		try:
			# First, open connection to zookeeper
			self.zk.start ()

			# if the zookeeper Parent Path exists then get the children information
			if self.zk.exists (self.ppath):
				children = self.zk.get_children(self.ppath)

				# store the children info to the broker_list and sort it ascending order
				self.broker_list = children
				self.broker_list.sort()

				# store the first child path to the leader_path
				leader_path = self.ppath +str("/")+str(self.broker_list[0])
				print("Leader Path: "+leader_path)

				# if the length of children is bigger than 1 
				# store the second children's path to the candidate_path
				if len(children) > 1:
					candidate_path = self.ppath +str("/")+str(self.broker_list[1])
					print("Candidate Path: "+candidate_path)
				else:
					# if the length of children is 1(only child) then 
					# candidate_path equals leader_path
					candidate_path = leader_path
					print("Candidate Path: "+candidate_path)

				# get the Socket Address of the Leader
				value, stat = self.zk.get (leader_path)
				self.brokeraddress = str(value.decode())
				print("Broker Leader IP: "+self.brokeraddress)
				
			else:
				print("exit")
				exit(0)
		except:
			print("Unexpected error in ClientApp::run", sys.exc_info()[0])
			raise


		# first parse the command line arguments
		parsed_args = parseCmdLineArgs ()

		# Get the Host name and IP of the server
		#host_name = self.gethostname() 
		#host_ip = self.gethostbyname(host_name) 

		# Get the process id of the process to use as the unique socket ID
		pid = os.getpid()
		pid_str = str(pid)

		# Topic is randomly chosen
		# topic = random.choice(["sports", "music", "weather"])
		topic = "sports"
		print("Process ID:"+pid_str+", Interested Topic : "+topic)


		# register the subscriber to the broker
		print("Register Subscriber(topic: "+str(topic)+", process id: "+pid_str+", brokeraddress: "+self.brokeraddress+")")

		self.context = zmq.Context()
		self.socket = self.context.socket(zmq.REQ)
		self.socket.setsockopt(zmq.IDENTITY, pid_str.encode()) # set the id of the socket
		self.socket.connect(self.brokeraddress) # connect
		self.socket.send_multipart([b"2",pid_str.encode(),topic.encode()])

		result = self.socket.recv_multipart()

		# Watch the children of the Broker parent Path
		@self.zk.ChildrenWatch (self.ppath)
		def child_change_watcher (children):

			print(str(children))
			children_path = self.ppath+str("/")+str(children[0])
			print("children_path: "+children_path)

			
			if leader_path == children_path :
				# if leader_path == children_path then it means there is just 1 broker
				# then do nothing
				print("Leader==children")
			else:
				# if there is child that can replace the leader
				# Get the socket address of the candidate
				value, stat = self.zk.get (children_path)
				self.brokeraddress = str(value.decode())
				print("IP of New Leader: "+self.brokeraddress)
			
				# Set the reconnect_flag 'True' so that reconnection is made
				self.reconnect_flag = True

		if parsed_args.mode=="D": # Directly receive the messages from the publishers
			self.socket.close()
			self.context = zmq.Context()
			self.socket = self.context.socket(zmq.REP)
			bind_address = "tcp://*:"+pid_str
			self.socket.bind(bind_address)

			while True:
				print("\nWait on  socket.recv_multipart("+bind_address+", topic: "+topic+")")
				message = self.socket.recv()
				print("Directly received message: "+message.decode())
				self.socket.send(b"Thank you publisher")


		else: # if argument is not 'D' then start waiting for the published contents
	
			self.socket.close()
			self.context = zmq.Context()
			self.socket = self.context.socket(zmq.DEALER)

			
			if self.first_flag == True:
				print("Make connection to the broker: "+self.brokeraddress)
				pid_str = "Wait"+pid_str # make the socket identification by adding "Wait" to the front of the process id
				
				# Set the Timeout value to 10000(10 seconds) to avoid endless loop
				self.socket.setsockopt(zmq.RCVTIMEO, 10000) # set the id of the socket
				self.socket.setsockopt(zmq.IDENTITY, pid_str.encode()) # set the id of the socket
				self.socket.connect(self.brokeraddress) # connect
				print("Connect (pid: "+pid_str+") to the broker")
				self.first_flag = False


			while True:
				print("\n\n\nwait_for_published_topic()")
				try:
					message = self.socket.recv_multipart()
					print("Received Contents: "+str(message))
				except:
					#print("Timeout occurred")
					if self.reconnect_flag == True:
						print("Making Reconnection to : "+self.brokeraddress)
						#connect_pub(pid_str, self.brokeraddress) # Register the publisher to the broker
						self.socket.close()
						self.context = zmq.Context()
						self.socket = self.context.socket(zmq.DEALER)
						self.socket.setsockopt(zmq.RCVTIMEO, 5000) # set the timeout value
						self.socket.setsockopt(zmq.IDENTITY, pid_str.encode()) # set the id of the socket
						self.socket.connect(self.brokeraddress) # connect
						self.reconnect_flag = False


if __name__ == "__main__":
	print("Demo program for ZooKeeper-based Barrier Sync: Client Appln")
	parsed_args = parseCmdLineArgs ()

	sub = Sub_Driver (parsed_args)

	# initialize the client
	sub.init_sub ()
	sub.run_sub ()
