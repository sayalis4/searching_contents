import socket
import sys
import random
import os
from threading import Thread
import numpy as np
import time

bootstrap_ip = "129.82.46.190"
bootstrap_port = 10000
server_address = (bootstrap_ip, bootstrap_port)
port = 20000
filename = "death"
command = ""
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
neighbour = []
neighbours = []
search = []
ip = socket.gethostbyname(socket.gethostname())
random_files = []
value = 0
status = True
doc = open('/Users/sayalishirode/Documents/ECE658/Lab03/'+str(port), 'w+')

def parse_arguments():
    global port, bootstrap_ip, bootstrap_port
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i] == '-p':
            port = int(sys.argv[i + 1])
            i = i + 1
        elif sys.argv[i] == '-b':
            bootstrap_ip = sys.argv[i + 1]
            i = i + 1
        elif sys.argv[i] == '-n':
            bootstrap_port = int(str(sys.argv[i + 1]))
            i = i + 1
        else:
            print "Usage :- ~$ <unstructpp> -p <portnum> -b <bootstrap ip> -n <bootstrap port> -h <help>"


def connect_to_bootstrap():
    global port, bootstrap_ip, bootstrap_port, ip, server_address, random_files
    f = open("/Users/sayalishirode/Documents/ECE658/Lab03/resources.txt", "r")
    a = []
    for line in f:
        a.append(line.strip())
    index = [0,1,2]
    new_a = np.delete(a,index)
    random_files = random.sample(new_a,8)
    print "Files generated are: ", random_files

    #print "Rank is: ", new_a.index(line)

    server_address = (bootstrap_ip, bootstrap_port)
    print server_address
    #print port
    # ip = socket.gethostbyname(socket.gethostname())
    # ip = "129.82.46.207"

    #print ip
    total_message = get_register_message(ip, port)
    print total_message


    sock.sendto(total_message, server_address)
    print "Message sent"
    reply, server = sock.recvfrom(1024)
    print "received message: ", reply



    msg = reply.split()
    print tuple(msg)
    if msg[3] == '1':
        print tuple(msg[4:])
        bootstrap_ip1 = msg[4]
        port1 = int(msg[5])
        print bootstrap_ip1
        print port1
        server_address1 = (bootstrap_ip1, port1)
        total_join_nodes_message = get_join_nodes(ip, port)
        print total_join_nodes_message
        sock.sendto(total_join_nodes_message, server_address1)
    elif msg[3] == '2':
        print tuple(msg[4:6]), tuple(msg[6:8])
        bootstrap_ip2 = msg[4]
        port2 = int(msg[5])
        print bootstrap_ip2
        print port2
        server_address2 = (bootstrap_ip2, port2)
        bootstrap_ip3 = msg[6]
        port3 = int(msg[7])
        server_address3 = (bootstrap_ip3, port3)
        total_join_nodes_message = get_join_nodes(ip, port)
        # total_join_nodes_reply = reply_join_nodes()
        sock.sendto(total_join_nodes_message, server_address2)
        sock.sendto(total_join_nodes_message, server_address3)

    elif msg[3] == '3':
        print tuple(msg[4:6]), tuple(msg[6:8]), tuple(msg[8:10])
        # print type(tuple(msg[4:6]))
        bootstrap_ip4 = msg[4]
        port4 = int(msg[5])

        print bootstrap_ip4
        print port4
        server_address4 = (bootstrap_ip4, port4)
        bootstrap_ip5 = msg[6]
        port5 = int(msg[7])
        server_address5 = (bootstrap_ip5, port5)
        bootstrap_ip6 = msg[8]
        port6 = int(msg[9])
        server_address6 = (bootstrap_ip6, port6)

        total_join_nodes_message = get_join_nodes(ip, port)

        print total_join_nodes_message
        sock.sendto(total_join_nodes_message, server_address4)
        sock.sendto(total_join_nodes_message, server_address5)
        sock.sendto(total_join_nodes_message, server_address6)
        print "Nodes are joined"

        print "received message when nodes are joined: "



def get_register_message(ip, port):
    message = " REG " + str(ip) + " " + str(port) + " sayalishirode"
    length = len(message) + 4
    lengthInString = str("{0:0=4d}".format(length))
    total_message = lengthInString + message
    return total_message


def get_unregister_message(ip, port):
    unregister_message = " DEL IPADDRESS " + str(ip) + " " + str(port) + " sayalishirode"
    leng = len(unregister_message) + 4
    lengthInString = str("{0:0=4d}".format(leng))
    total_unregister_message = lengthInString + unregister_message
    return total_unregister_message


def get_delete_user():
    delete_user = " DEL UNAME" + " sayalishirode"
    length_of_uname_message = len(delete_user) + 4
    lengthInString = str("{0:0=4d}".format(length_of_uname_message))
    total_uname_message = lengthInString + delete_user
    return total_uname_message


def get_join_nodes(ip,port):
    join_nodes = " JOIN " + str(ip) + " " + str(port)
    length_of_join_nodes = len(join_nodes) + 4
    lengthInString = str("{0:0=4d}".format(length_of_join_nodes))
    total_join_nodes_message = lengthInString + join_nodes
    return total_join_nodes_message


def reply_join_nodes(value):
    join_nodes_reply = " JOINOK " + str(value)
    length_join_nodes_reply = len(join_nodes_reply) + 4
    lengthInString = str("{0:0=4d}".format(length_join_nodes_reply))
    total_join_nodes_reply = lengthInString + join_nodes_reply
    return total_join_nodes_reply


def get_leave_nodes(ip, port):
    leave_nodes = " LEAVE " + str(ip) + " " + str(port)
    length_of_leaving_nodes = len(leave_nodes) + 4
    lengthInString = str("{0:0=4d}".format(length_of_leaving_nodes))
    total_leave_nodes_message = lengthInString + leave_nodes
    return total_leave_nodes_message

def reply_leave_nodes():
    leave_nodes_reply = " LEAVEOK " + str(0)
    length_leave_nodes_reply = len(leave_nodes_reply) + 4
    lengthInString = str("{0:0=4d}".format(length_leave_nodes_reply))
    total_leave_nodes_reply = lengthInString + leave_nodes_reply
    return total_leave_nodes_reply

def get_search_nodes(ip, port,filename, hops):
    search_nodes = " SER " + str(ip) + " " + str(port) + " " + str(filename) + " " + str(hops)
    length_of_search_nodes = len(search_nodes) + 4
    lengthInString = str("{0:0=4d}".format(length_of_search_nodes))
    total_search_nodes_message = lengthInString + search_nodes
    return total_search_nodes_message

def reply_search_nodes(ip1, port1,filename1, hops1):
    search_nodes_reply = " SEROK " + str(ip1) + " " + str(port1) + " " + str(hops1) + " " + str(filename1)
    length_search_nodes_reply = len(search_nodes_reply) + 4
    lengthInString = str("{0:0=4d}".format(length_search_nodes_reply))
    total_search_nodes_reply = lengthInString + search_nodes_reply
    return total_search_nodes_reply


class Terminate(Thread):
    def run(self):
        # sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        global command, sock, server_address, ip, port, random_files
        while True:
            command = raw_input()
            if command == "leave":
                total_leave_message = get_leave_nodes(ip,port)
                print "leave message : ",total_leave_message
                for node in neighbours:
                    #print type(neighbours)
                    sock.sendto(total_leave_message, node)
                total_unregister_message = get_unregister_message(ip, port)
                print "unregister message: ", total_unregister_message
                sock.sendto(total_unregister_message, server_address)
                sock.close()
                os._exit(1)
            elif command == "leaveall":
                total_uname_message = get_delete_user()
                sock.sendto(total_uname_message, server_address)

                print "User deleted"
                return


            elif command == "neighbours":
                print neighbours
            elif command == "search":
                total_search_nodes_message = get_search_nodes(ip,port,filename, 0)
                print total_search_nodes_message
                #total_search_message = get_search_nodes(ip,port)
                found = False
                for line in random_files:
                    if filename.lower() in line.lower():
                        print 'file found in own ', (line)
                        found = True
                if found is True:
                    return
                for neighbour in neighbours:
                    #print total_search_nodes_message
                    #sock.sendto(filename,neighbour)
                    sock.sendto(total_search_nodes_message,neighbour)


class Receive(Thread):
    def run(self):
        global sock, neighbours, random_files, neighbour, ip, port, value, status, doc
        hops = 0

        while status:
            reply, server = sock.recvfrom(1024)
            msg = reply.split()
            print "Received", msg, server


            if msg[1] == "JOIN":
                value = 0
                for neighbour in neighbours:
                    #print neighbour
                    if neighbour == server:
                        value = 9999
                        total_join_nodes_reply = reply_join_nodes(value)
                        sock.sendto(total_join_nodes_reply, server)
                        break
                total_join_nodes_reply = reply_join_nodes(value)
                sock.sendto(total_join_nodes_reply, server)
                neighbours.append(server)

            elif msg[1] == "JOINOK":
                if msg[2] == '0':
                    neighbours.append(server)
                elif msg[2] == '9999':
                    print "Error while joining"
                else:
                    print "unknown join ok reply"

            elif msg[1] == "LEAVE":
                total_leave_nodes_reply = reply_leave_nodes()
                for i,node in enumerate(neighbours):
                    if server == node:
                        sock.sendto(total_leave_nodes_reply,node)
                        del neighbours[i]
            # elif msg[1] == "LEAVEOK":
            #     for i,node in enumerate(neighbours):
            #         if server == node:
            #             del neighbours[i]
            elif msg[1] == "DEL":
                print "UNREGISTER Successful"
                sock.close()
                os._exit(1)
            elif msg[1] == "SEROK":
                doc.write(reply + " " + str(time.time()) + "\n")
                print "file found at", server
            elif msg[1] == "SER":
                ip_node = msg[2]
                port_node = msg[3]
                address = (ip_node,int(port_node))
                hops = int(msg[-1])
                hops = hops + 1
                found = False
                query = ""
                i = 4
                while i < len(msg) - 1:
                    query += msg[i]
                    query += " "
                    i = i + 1
                for line in random_files:
                    #print query.lower().strip(), line.lower().strip()
                    if query.lower().strip() == line.lower().strip():
                        #print(filename)
                        total_search_nodes_reply = reply_search_nodes(ip,port,line, hops)
                        print "Query found, reply sending", total_search_nodes_reply, "to address", address
                        sock.sendto(total_search_nodes_reply,address)
                        found = True
                        break
                if found is True:
                     continue
                reply = get_search_nodes(msg[2],msg[3],query, hops)
                for neighbour in neighbours:
                    #print "neighbour: ",neighbour
                    #print "server: ", server
                    #print "address: ",address
                    if neighbour != server and neighbour != address:
                        #print "sending forward"
                        sock.sendto(reply,neighbour)


class Queries(Thread):
    def run(self):
        number_of_queries = 5
        resources = 160
        #random_nodes = []
        #for x in range(number_of_queries):
        g = open("/Users/sayalishirode/Documents/ECE 658/Lab03/nodes.txt", "r")
        b = []
        for line in g:
            #print line
            b.append(line.strip())
        #random_nodes = random.sample(b,20)
        print "Nodes generated are: ", random.sample(b,20)
        x = number_of_queries * resources
        print x

ports = [20001, 20002, 20003, 20004, 20005, 20006, 20007, 20008, 20009, 20010, 20011, 20012, 20013, 20014, 20015, 20016, 20017, 20018, 20019, 20020]
def get_zipf_h(size, s):
    harmonic = 0
    for i in range(1, size + 1):
        harmonic += (1.0/pow(i, s))
    return harmonic


def getpI(i, s, harmonic):
    print 1.0/(pow(i,s)*harmonic)
    return 1.0/(pow(i,s)*harmonic)


def get_search_nodes_query(port,filename, hops):
    ip = '10.0.0.139'
    search_nodes = " SER " + str(ip) + " " + str(port) + " " + str(filename) + " " + str(hops)
    length_of_search_nodes = len(search_nodes) + 4
    lengthInString = str("{0:0=4d}".format(length_of_search_nodes))
    total_search_nodes_message = lengthInString + search_nodes
    return total_search_nodes_message

def run():
    number_of_queries = 5
    num_resources = 160
    s =0.6
    num_nodes = 20
    resources = []
    #for x in range(number_of_queries):
    node_files = open("/Users/sayalishirode/Documents/ECE658/Lab03/nodes.txt", "r")
    resources_files = open("/Users/sayalishirode/Documents/ECE658/Lab03/resources.txt", "r")
    nodes = []
    for line in node_files:
        #print line
        nodes.append(line.strip())
    for line in resources_files:
        if line.startswith("#"):
            continue
        resources.append(line.rstrip())
    x = number_of_queries * num_resources
    print x
    harmonic = get_zipf_h(num_resources, s)
    print harmonic
    pI = []
    for i in range(1, num_resources+1):
        pI.append(getpI(i, s, harmonic))
    global port
    for resource in resources:
        random_nodes = random.sample(ports, 5)

        for i, node in enumerate(random_nodes):
            random_senders = random.sample(ports, 5)
            message = get_search_nodes_query(port, resource, 0)
            sock.sendto(message, ('10.0.0.139', random_senders[i]))


def check_status():
    global status
    time.sleep(20)
    status = False


if __name__ == "__main__":


    #doc.write("hello")
    parse_arguments()
    doc = open('/Users/sayalishirode/Documents/ECE658/Lab03/'+str(port)+ "/log", 'w+')
    sock.bind(('', port))
    connect_to_bootstrap()
    termination = Terminate()
    termination.start()
    r = Receive()
    r.start()
    time.sleep(.5)
    run()
    check_status()
    #doc.close()
    time.sleep(8)
    #q = Queries()
    #q.start()