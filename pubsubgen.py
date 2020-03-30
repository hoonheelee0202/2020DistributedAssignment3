import os
import sys, stat
import random
import argparse   # argument parser

##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
	parser = argparse.ArgumentParser ()

    # add positional arguments in that order
	parser.add_argument ("mode",  help="I:Indirect, D:Direct")
	parser.add_argument ("brokernum", help="Number of Brokers generating")
	parser.add_argument ("pubnum", help="Number of Publishers generating")
	parser.add_argument ("subnum", help="Number of Subscribers generating")
	parser.add_argument ("auto", help="Broker Topic Auto Generating Y/N")

    # parse the args
	args = parser.parse_args ()

	return args


def main():
	print("Publisher/Subscriber Generator started..")

	# parse the command line
	parsed_args = parseCmdLineArgs ()
	
	broker_file = open("broker_command.txt","w")
	pub_file = open("publisher_command.txt","w")
	sub_file = open("subscriber_command.txt","w")

	for i in range(int(parsed_args.brokernum)):
		cmd_str = "python3 broker.py broker"+str(i)+" "+str(5559+i)+"&\n"
		broker_file.write(cmd_str)

	broker_file.close()
	file_path=os.getcwd()+"/broker_command.txt"
	os.chmod(file_path, 0o777) # for example

	for i in range(int(parsed_args.pubnum)):
		cmd_str = "python3 pub.py "+parsed_args.mode+" "+parsed_args.auto+"&\n"
		pub_file.write(cmd_str)

	
	pub_file.close()
	file_path=os.getcwd()+"/publisher_command.txt"
	#os.chmod(file_path, stat.S_IEXEC)
	os.chmod(file_path, 0o777) # for example

	for i in range(int(parsed_args.subnum)):
		cmd_str = "python3 sub.py "+parsed_args.mode+"&\n"
		sub_file.write(cmd_str)
	
	sub_file.close()
	file_path=os.getcwd()+"/subscriber_command.txt"
	os.chmod(file_path, 0o777) # for example

	print("Broker/Publisher/Subscriber Generator Ended..")
	


if __name__ == "__main__":
	main()
