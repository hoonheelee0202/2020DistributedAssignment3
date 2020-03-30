import sys
import zmq
import time
import socket
#import mysql.connector
from middleware import db_connect
#from middleware import frontend

import pymysql.cursors


# argument parser
import argparse

# Now import the kazoo package that supports Python binding
# to ZooKeeper
from kazoo.client import KazooClient

# Global Varible Declaration 
publisher = [] # topic, pid
subscriber = [] # topic, pid
search_list = [] # topic, pid

#--------------------------------------------------------------------
# Append the publisher to the list 'publisher'
# register_pub(topic,socket_id)
def append_publisher(topic,socket_id):
	publisher.append([topic,socket_id])

#--------------------------------------------------------------------
# Append the Subscriber to the list 'subscriber'
# register_sub(topic,socket_id)
def append_subscriber(topic,socket_id):
	subscriber.append([topic,socket_id])

def get_Host_name_IP(): 
	try: 
		host_name = socket.gethostname() 
		host_ip = socket.gethostbyname(host_name) 
		print("Hostname :  ",host_name) 
		print("IP : ",host_ip) 
	except: 
		print("Unable to get Hostname and IP") 

##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
	# parse the command line
	parser = argparse.ArgumentParser ()

	# add optional arguments
	parser.add_argument ("-a", "--zkIPAddr", default="127.0.0.1", help="ZooKeeper server ip address, default 127.0.0.1")
	parser.add_argument ("-c", "--cond", type=int, default=5, help="Barrier Condition representing number of client apps in the barrier, default 5")
	parser.add_argument ("-p", "--zkPort", type=int, default=2181, help="ZooKeeper server port, default 2181")
	parser.add_argument ("-ppath", "--zkPpath", default="/broker", help="ZooKeeper Broker Parent Path")
	parser.add_argument ("-topicpath", "--topicPath", default="/topic", help="ZooKeeper Topic Path")

	parser.add_argument ("name", help="Broker Name")
	parser.add_argument ("port", help="Broker Port Number")

	# command example: python3 broker.py broker1 5559

	# parse the args
	args = parser.parse_args ()

	return args
    

class Broker_Driver ():

	# constructor
	def __init__ (self, args):
		self.name = args.name
		self.zkIPAddr = args.zkIPAddr  # ZK server IP address
		self.zkPort = args.zkPort # ZK server port num
		self.cond = args.cond # used as barrier condition
		self.ppath = args.zkPpath # refers to the parent znode path
		self.topicPath = args.topicPath # refers to the parent znode path
		self.brokerAddressPort = args.port  # indicating if barrier has reached
		self.zk = None  # session handle to the server
		
    # override the run method which is invoked by start
	def init_broker (self):
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

	def run_broker(self):
		try:
			# First, open connection to zookeeper
			self.zk.start ()


			if (self.zk.exists (self.ppath)) :
				# if parent path already exists, set first_flat 'False'
				first_flag=False
			else:
				# if parent path does not exist, create the Parent Path and set first_flag 'True'
				# if first_flag is True then delete the tables' contents(publisher, subscriber)
				print("Create the znode Path: "+self.ppath)
				self.zk.create (self.ppath, value=self.name.encode())
				first_flag=True

			# set the broker_path so that children node can be created
			broker_path = self.ppath + str("/") + "guid-n_"
			broker_path = self.zk.create (broker_path, value=self.name.encode(), ephemeral=True, sequence=True)

			
			# Topic Path Creation if not exists
			if ( self.zk.exists (self.topicPath)) :
				print("Path("+self.topicPath+") already exists")
			else :
				# if topic path does not exist, create the Topic Path
				print("Create the Topic Path: "+self.topicPath)
				self.zk.create (self.topicPath, value=b"topic")
			


    
		except:
			print("Unexpected error in ClientApp::run", sys.exc_info()[0])
			raise

		# Get the Host name and IP of the server
		host_name = socket.gethostname() 
		host_ip = socket.gethostbyname(host_name) 

		search_list = []


		mydb = db_connect()
		mycursor = mydb.cursor()

		#print("First_flag: "+str(first_flag))
		# Table Initialization if first_flag is True
		if first_flag == True:
			#print("delete execution")
			mycursor.execute("DELETE FROM publisher")
			mycursor.execute("DELETE FROM subscriber")
			mycursor.execute("DELETE FROM brokerrecord")
			mydb.commit()
		


		# Prepare our context and sockets
		context = zmq.Context()
		frontend = context.socket(zmq.ROUTER)
		bindAddress = "tcp://*:"+self.brokerAddressPort
		frontend.bind(bindAddress)

		brokeraddress = "tcp://"+host_ip+":"+self.brokerAddressPort

		# store the BrokerAddress to the children node
		self.zk.set (broker_path, str(brokeraddress).encode ())
		brokeraddress = "tcp://"+host_ip+":"+self.brokerAddressPort
		print("Broker Zookeeper path: "+broker_path)
		print("Broker Address: "+brokeraddress)


		# Initialize poll set
		poller = zmq.Poller()
		poller.register(frontend, zmq.POLLIN)

		
		# Initialize message_type Variable
		# message_type
		# '1' : publisher Registration(to Mysql DB Table-publisher)
		# '2' : Subscriber Registration(to Mysql DB Table-subscriber)
		# '3' : Publish message through Broker
		message_type = "0"

		# Switch messages between sockets
		while True:
			print("\nWaiting for the message to be received..")
			socks = dict(poller.poll())
			
			if socks.get(frontend) == zmq.POLLIN:
				message = frontend.recv_multipart()
				
				message_type = message[2].decode()
				socket_id = message[3].decode()
				topic = message[4].decode()

				print("\n\n\nReceived msg_type: "+message_type+", socket_id: "+socket_id+", topic: "+topic)
				
				if message_type == "1" : # request to register the publisher
					append_publisher(topic, socket_id)
					# DB Insert to the table 'publisher'
					sql = "INSERT INTO publisher VALUES (%s, %s)"
					val = (topic, socket_id)
					
					mycursor.execute(sql, val)
					mydb.commit()
					print("\nInsert into publisher("+topic+","+socket_id+")")
					#print "SQL execution: "+sql
					
					# Register the publisher's topic to the Zookeeper
					# set the topic_path so that children node can be created
					topic_path = self.topicPath + str("/") + topic

					
					if (self.zk.exists (topic_path)) :
						# if topic already exists then add new 
						print("Topic("+topic_path+") already exists");
					else:
						# if parent path does not exist, create the Parent Path and set first_flag 'True'
						# if first_flag is True then delete the tables' contents(publisher, subscriber)
						print("Create the topic Path: "+topic_path)
						self.zk.create (topic_path, value=topic.encode())

					# set the broker_path so that children node can be created

					topic_path = topic_path + str("/") + topic+"_"					
					topic_path = self.zk.create (topic_path, value=socket_id.encode(), ephemeral=True, sequence=True)


					# send bakc the reply
					#frontend.send_multipart([socket_id.encode(),b"",b"Thank you 1"])
					# return the created topic_path
					frontend.send_multipart([socket_id.encode(),b"",topic_path.encode()])
				elif message_type == "2" :  # request to register the subscriber
					append_subscriber(topic, socket_id)
					# DB Insert to the table 'subscriber'
					mycursor = mydb.cursor()
					sql = "INSERT INTO subscriber VALUES (%s, %s)"
					val = (topic, socket_id)
					mycursor.execute(sql, val)

					mydb.commit()
					print("\nInsert into subscriber("+topic+","+socket_id+")")
					# send bakc the reply
					frontend.send_multipart([socket_id.encode(),b"",b"Thank you 2"])
				elif message_type == "3" :
					#search_list = find_brute(subscriber, topic)
					contents = str(message[5])
					print("\nBroker publishing the message("+contents+") to the subscribers")
					mycursor = mydb.cursor()
					sql = "SELECT * FROM subscriber WHERE topic='"+topic+"'"
					#print(sql)

					mycursor.execute(sql)
					myresult = mycursor.fetchall()
					
					#print "Len of myresult"+str(len(myresult))
					if len(myresult)!=0 : # Add the searched DB result to the search_list
						for x in myresult:
							search_list.append([x[0],x[1]])

						# send the return Messages to the subscribers
						for i in range(len(search_list)):
							return_socket_id = "Wait"+search_list[i][1] #Make the socket_id by adding "Wait" to the front
							#print("return topic: "+topic+", return socket: "+return_socket_id)
							frontend.send_multipart([return_socket_id.encode(),b"",contents.encode()])
							frontend.send_multipart([socket_id.encode(),b"",b"Thank you 3"])
						
						# Initialize the search_list
						search_list = []
					else:
						print("\nNo matched subscriber to receive the contents")
						frontend.send_multipart([socket_id.encode(),b"",b"Thank you 3"])	


if __name__ == "__main__":

	print("----- Pub/Sub Broker using Zookeeper -----")
	parsed_args = parseCmdLineArgs ()

	broker = Broker_Driver (parsed_args)

	# initialize the client
	broker.init_broker ()
	broker.run_broker ()
