import zmq
import os
import random
import threading
import time
import sys
from middleware import register_pub
from middleware import connect_pub
from middleware import register_sub
from middleware import publish
from middleware import db_connect
from datetime import datetime
#from broker import frontend

import pymysql.cursors

import argparse   # argument parser

# Now import the kazoo package that supports Python binding
# to ZooKeeper
from kazoo.client import KazooClient


# Global Varible Declaration 
search_list = [] # topic, pid

zkConnection = None
global_topic_path = ""

#reconnect_flag=False

from signal import signal, SIGINT
from sys import exit

def handler(signal_received, frame):
	# Handle any cleanup here
	print("global Topic Path: "+global_topic_path)
	print('SIGINT or CTRL-C detected. Exiting gracefully')
	# zookeeper stop
	zkConnection.delete(global_topic_path, recursive = True)
	zkConnection.stop ()
	
	exit(0)


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
	parser.add_argument ("-topicpath", "--topicPath", default="/topic", help="ZooKeeper Topic Path")

    # add positional arguments in that order
	parser.add_argument ("mode", help="D:Directly send the contents to the subscriber, I:Send the contents to the broker")
	parser.add_argument ("auto")

	# command example : python3 pub.py I Y

    # parse the args
	args = parser.parse_args ()

	return args

class Pub_Driver ():

	# constructor
	def __init__ (self, args):
		self.zkIPAddr = args.zkIPAddr  # ZK server IP address
		self.zkPort = args.zkPort # ZK server port num
		self.ppath = args.zkPpath # refers to the parent znode path
		self.topicPath = args.topicPath # refers to the Topic Path
		self.zk = None  # session handle to the server
		self.first_flag = True
		self.reconnect_flag = False
		self.context = zmq.Context()
		self.socket = self.context.socket(zmq.REQ)
		self.broker_list = []
		self.topic_list = []
		zkConnection = None

	# Initialize the variables
	def init_pub (self):
		# It is here that we connect with the zookeeper server
		# and see if the condition has been met
		try:
			# Instantiate zookeeper client object
			# right now only one host; it could be the ensemble
			hosts = self.zkIPAddr + str (":") + str (self.zkPort)
			#self.zk = KazooClient (hosts)
			zkConnection = KazooClient (hosts)
			#zkConnection = self.zk

		except:
			print("Unexpected error in ClientApp::run", sys.exc_info()[0])
			raise

	def run_pub(self):

		global zkConnection
		global global_topic_path
	
		try:

			# Get the Host name and IP of the server
			#host_name = self.socket.gethostname() 
			#host_ip = self.socket.gethostbyname(host_name) 

			mydb = db_connect()
			mycursor = mydb.cursor()

			# First, open connection to zookeeper
			hosts = self.zkIPAddr + str (":") + str (self.zkPort)
			zkConnection = KazooClient (hosts)
			zkConnection.start ()

			# if the zookeeper Parent Path exists then get the children information
			if zkConnection.exists (self.ppath):
				children = zkConnection.get_children(self.ppath)

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
				value, stat = zkConnection.get (leader_path)
				self.brokeraddress = str(value.decode())
				print("Broker Leader IP: "+self.brokeraddress)
				
			else:
				print("exit")
				exit(0)
		except:
			print("Except")
			print("Unexpected error in ClientApp::run", sys.exc_info()[0])
			raise

		# Initialize the search_list
		search_list = []

		# Prepare our context and sockets
		context = zmq.Context()

		# first parse the command line arguments
		parsed_args = parseCmdLineArgs ()
		
		# Get the process id of the process to use as the unique socket ID
		pid = os.getpid()
		pid_str = str(pid)


		# Watch the children of the Broker parent Path
		@zkConnection.ChildrenWatch (self.ppath)
		def child_change_watcher (children):
			print("Driver::run -- children watcher: num childs = {}")
			temp_broker_list = children
			temp_broker_list.sort()
			children_path = self.ppath+str("/")+str(temp_broker_list[0])
			print("children_path: "+children_path)

			
			if leader_path == children_path :
				# if leader_path == children_path then it means there is just 1 broker
				# then do nothing
				print("Leader==children")
			else:
				# if there is child that can replace the leader
				# Get the socket address of the candidate
				value, stat = zkConnection.get (children_path)
				self.brokeraddress = str(value.decode())
				print("IP of New Leader: "+self.brokeraddress)
			
				# Set the reconnect_flag 'True' so that reconnection is made
				self.reconnect_flag = True

		# Parse the brokeraddressIP
		brokeraddressIP_list = self.brokeraddress.split(":")
		brokeraddressIP = brokeraddressIP_list[1]
		brokeraddressIP = brokeraddressIP[2:]

		
		
		

		# Publish the contents
		if parsed_args.mode == "D":
			print("Directly send the contents to Subscribers without going through broker")
			
			while True:

				# if auto option is Yes, randomly choose topic and contents		
				if parsed_args.auto == "Y":
					# Topic is randomly chosen
					# topic = random.choice(["sports", "music", "weather"])
					topic = "sports"
					
					# Contents is randomly chosen
					contents = random.choice(["Basketball", "Jazz", "Sunny"])
					print("Publishing Topic: "+topic+", Contents: "+contents)
				elif parsed_args.auto == "N":
					topic = input("Enter the topic : ") 
					contents = input("Enter the contents : ")

				# if first_flat is 'True' make connection to the broker and 
				# register the publisher to the broker(message_type '1')
				if self.first_flag == True:
					print("Make connection to the broker: "+self.brokeraddress)
					self.socket.setsockopt(zmq.IDENTITY, pid_str.encode()) # set the id of the socket
					self.socket.connect(self.brokeraddress) # connect
					self.socket.send_multipart([b"1",pid_str.encode(),topic.encode()])
					print("Register the publisher(pid: "+pid_str+", topic: "+topic+") to the broker")
					result = self.socket.recv_multipart()
					#print("Register_pub result:"+str(result))
					global_topic_path = result[0].decode()
					print("global_topic_path: "+global_topic_path)

					# Set the first_flag 'False' so that this does not execute again
					self.first_flag = False




				# check if this publisher has the highest priority judged by the zookeeper
				topic_path = self.topicPath + str("/") + topic
				# Topic Path Creation if not exists
				if ( zkConnection.exists (topic_path)) :
					print("Path("+topic_path+") exists. Check the priority")

					
					children = zkConnection.get_children(topic_path)

					# store the children info to the topic_list and sort it ascending order
					self.topic_list = children
					self.topic_list.sort()

					# store the first child path to the leader_path
					topic_leader_path = topic_path +str("/")+str(self.topic_list[0])
					print("topic_leader_path: "+topic_leader_path)

					# if the length of children is bigger than 1 
					# store the second children's path to the candidate_path
					if len(children) > 1:
						topic_candidate_path = topic_path +str("/")+str(self.topic_list[1])
						print("topic_candidate_path: "+topic_candidate_path)
					else:
						# if the length of children is 1(only child) then 
						# candidate_path equals leader_path
						topic_candidate_path = topic_leader_path
						print("topic_candidate_path: "+topic_candidate_path)

					# get the Socket Address of the Leader
					value, stat = zkConnection.get (topic_leader_path)
					leaderSocketId = str(value.decode())
					print("Topic Leader Socket ID: "+leaderSocketId)

					if pid_str == leaderSocketId :
						print("Pid of this Publisher("+pid_str+") is the same as the topic leader's pid("+leaderSocketId+")")
					else :
						print("Pid of this Publisher("+pid_str+") is not the same as the topic leader's pid("+leaderSocketId+")")
						print("Execute Continue")
						time.sleep(3)
						continue
				





				mydb = db_connect()
				mycursor = mydb.cursor()
				sql = "SELECT * FROM subscriber WHERE topic='"+topic+"'"
				#print("SQL: "+sql)
				mycursor.execute(sql)
				myresult = mycursor.fetchall()
				for x in myresult:
					#print(x)
					search_list.append([x[0],x[1]])

				print(str(search_list))


				# send the return Messages to the subscribers
				for i in range(len(search_list)):

					self.socket.close()
					connect_address ="tcp://localhost:"+search_list[i][1]
					#connect_address ="tcp://"+brokeraddressIP+":"+search_list[i][1]

						
					try:
						#print("connect_address: "+connect_address)
						self.socket = self.context.socket(zmq.REQ)
						self.socket.setsockopt(zmq.RCVTIMEO, 1000) # set the timeout of the socket
						self.socket.setsockopt(zmq.IDENTITY, pid_str.encode()) # set the id of the socke
					
						self.socket.connect(connect_address)
						start_time = time.time ()
						#print("Send the message: "+contents)
						self.socket.send(contents.encode())
						message = self.socket.recv()
						end_time = time.time()

						# Insert the message record
						sql = "INSERT INTO brokerrecord VALUES (%s, %s, %s, %s, %s, %s)"
						val = (pid_str, self.brokeraddress, topic, contents, str(datetime.now()), str(end_time-start_time))
						#print("Insert : "+sql)
						mycursor.execute(sql, val)
						mydb.commit()

						#print("Returned message: "+message.decode()+", Elapsed Time: "+str(end_time-start_time))
					except:
						print("Connection Failure to: "+connect_address)
						continue


					self.socket.close()
					
				
				# Initialize the search_list
				search_list = []
				mycursor.close()
				mydb.close()

				time.sleep(3)

		else:
			print("send the contents to Subscribers using the Broker")

			while True:

				# if auto option is Yes, randomly choose topic and contents		
				if parsed_args.auto == "Y":
					# Topic is randomly chosen
					# topic = random.choice(["sports", "music", "weather"])
					topic = "sports"
					#print "publishing Topic: "+topic
					# Contents is randomly chosen
					contents = random.choice(["Good morning", "Good Afternoon", "Good Evening"])
					print("Publishing Topic: "+topic+", Contents: "+contents)
				# if auto option is 'N' then put in the topic and contents
				elif parsed_args.auto == "N":
					topic = input("Enter the topic : ") 
					contents = input("Enter the contents : ")


				# if first_flat is 'True' make connection to the broker and 
				# register the publisher to the broker(message_type '1')
				if self.first_flag == True:
					print("Make connection to the broker: "+self.brokeraddress)
					self.socket.setsockopt(zmq.IDENTITY, pid_str.encode()) # set the id of the socket
					self.socket.connect(self.brokeraddress) # connect
					self.socket.send_multipart([b"1",pid_str.encode(),topic.encode()])
					print("Register the publisher(pid: "+pid_str+", topic: "+topic+") to the broker")
					result = self.socket.recv_multipart()
					print("Register_pub result:"+str(result))
					global_topic_path = result[0].decode()
					print("global_topic_path: "+global_topic_path)

					# Set the first_flag 'False' so that this does not execute again
					self.first_flag = False


				# if reconnect_flag is set Make connection to the broker again
				# This time, broker is the candidate leader
				if self.reconnect_flag == True:
					print("Making Reconnection to the broker: "+self.brokeraddress)
					self.socket = self.context.socket(zmq.REQ)
					self.socket.setsockopt(zmq.IDENTITY, pid_str.encode()) # set the id of the socket
					self.socket.connect(self.brokeraddress) # connect

					# Set the reconnect_flag 'False' so that this does not execute again
					self.reconnect_flag = False

				print("\n\nPublish the contents to the subscribers\n")







				# check if this publisher has the highest priority judged by the zookeeper
				topic_path = self.topicPath + str("/") + topic
				# Topic Path Creation if not exists
				if ( zkConnection.exists (topic_path)) :
					print("Path("+topic_path+") exists. Check the priority")

					
					children = zkConnection.get_children(topic_path)

					# store the children info to the topic_list and sort it ascending order
					self.topic_list = children
					self.topic_list.sort()

					# store the first child path to the leader_path
					topic_leader_path = topic_path +str("/")+str(self.topic_list[0])
					print("topic_leader_path: "+topic_leader_path)

					# if the length of children is bigger than 1 
					# store the second children's path to the candidate_path
					if len(children) > 1:
						topic_candidate_path = topic_path +str("/")+str(self.topic_list[1])
						print("topic_candidate_path: "+topic_candidate_path)
					else:
						# if the length of children is 1(only child) then 
						# candidate_path equals leader_path
						topic_candidate_path = topic_leader_path
						print("topic_candidate_path: "+topic_candidate_path)

					# get the Socket Address of the Leader
					value, stat = zkConnection.get (topic_leader_path)
					leaderSocketId = str(value.decode())
					print("Topic Leader Socket ID: "+leaderSocketId)

					if pid_str == leaderSocketId :
						print("Pid of this Publisher("+pid_str+") is the same as the topic leader's pid("+leaderSocketId+")")
					else :
						print("Pid of this Publisher("+pid_str+") is not the same as the topic leader's pid("+leaderSocketId+")")
						print("Execute Continue")
						time.sleep(3)
						continue

				else :
					# if topic path does not exist, this publisher does not have the highest priority, so just execute 'continue'
					print("Execute Continue. topic does not exist")	
					time.sleep(3)		
					continue


				start_time = time.time ()
				#print("start_time: "+str(datetime.now()))
				try:
					self.socket.setsockopt(zmq.RCVTIMEO, 5000) # set the timeout
					self.socket.send_multipart([b"3",pid_str.encode(),topic.encode(),contents.encode()])
					print("Publish the contents(pid: "+pid_str+", topic: "+topic+", contents: "+contents+") to the broker")
					message = self.socket.recv_multipart()
					end_time = time.time()

					# Insert the message record
					sql = "INSERT INTO brokerrecord VALUES (%s, %s, %s, %s, %s, %s)"
					val = (pid_str, self.brokeraddress, topic, contents, str(datetime.now()), str(end_time-start_time))
					#print("Insert : "+sql)
					mycursor.execute(sql, val)
					mydb.commit()

					print("Returned message: "+str(message)+", Elapsed Time: "+str(end_time-start_time))
				except:
					# if there's no response from the server for a certain amount of time
					# Then, close the socket to the existing server
					# The program will find another broker server
					self.socket.close()
					print("No response from the server")

				time.sleep(3)


if __name__ == "__main__":
	print("Demo program for ZooKeeper-based Barrier Sync: Client Appln")
	parsed_args = parseCmdLineArgs ()

	signal(SIGINT, handler)

	pub = Pub_Driver (parsed_args)

	# initialize the client
	pub.init_pub ()
	pub.run_pub ()

