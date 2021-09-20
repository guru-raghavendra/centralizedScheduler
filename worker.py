# -*- coding: utf-8 -*-
"""
Created on Sat Nov 28 23:05:35 2020

@author: Aneesh
"""

import json
import socket
import time
from threading import Thread
from queue import Queue
import sys

BUFF_SIZE = 10000
task_port = 4000
update_port = 5001

worker_id = 1


if(len(sys.argv)>2):
	task_port=int(sys.argv[1])
	worker_id = int(sys.argv[2])


worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
worker_socket.bind(('localhost',task_port))

worker_socket.listen(1)
print('Worker node listening on port: ',task_port)
print('Worker id:',worker_id)


#worker_update_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


def task_requests(task_pool):
	# Create a connection
	worker_conn, addr = worker_socket.accept()
	
	while True:	
		task = worker_conn.recv(BUFF_SIZE)
		task = json.loads(task)
		
		print(task)
		task_pool.put(task)
		
		worker_conn.send((task['task_id']+' received').encode())


def update_task(task_pool):
	while task_pool.empty():
		pass
	
	while True:
		while(not task_pool.empty()):
			task = task_pool.get()
			time.sleep(1)
			task['duration']-=1
			if(task['duration']<1):
				print(task['task_id'], ' done')
				
				with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as worker_update_socket:
					worker_update_socket.connect(('localhost', update_port))
					worker_update_socket.send(json.dumps(task).encode())
					#worker_update_socket.recv(2)
			else:
				task_pool.put(task)
			
			for _ in range(task_pool.qsize()-1):
				task = task_pool.get()
				task['duration']-=1
				if(task['duration']<1):
					print(task['task_id'], ' done')
					with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as worker_update_socket:
						worker_update_socket.connect(('localhost', update_port))
						worker_update_socket.send(json.dumps(task).encode())
					#worker_update_socket.recv(2)
				else:
					task_pool.put(task)
	
task_pool = Queue()

task_requests_thread = Thread(target= task_requests, args=(task_pool, ))
update_task_thread = Thread(target= update_task, args=(task_pool, ))

task_requests_thread.start()
update_task_thread.start()




