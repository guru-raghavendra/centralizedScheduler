# -*- coding: utf-8 -*-
"""
Created on Sat Nov 28 23:05:13 2020

@author: Aneesh
"""
import sys
import json
import socket
import time
from threading import Thread,Lock
from queue import Queue
import random

BUFF_SIZE = 10000
requests_port = 5000
job_port = 4000
update_listen_port = 5001


dtask = {}
#Input of config file
filename = sys.argv[1]
c_file = open(filename,'r')
workers = json.loads(c_file.read())
c_file.close()
workers = workers['workers']
for i in range(len(workers)):
    workers[i]['free_slots']=workers[i]['slots']
    workers[i]['id']=i
#print(workers)

#Type of Scheduler
scheduler = sys.argv[2]

previously_selected = -1

#Server socket
main_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
main_socket.bind(('localhost', requests_port))

#Client sockets
job_sockets = []
for i in range(len(workers)):
    job_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    job_socket.connect(('localhost', int(workers[i]['port'])))
    job_sockets.append(job_socket)


print('Started Listening on port: ',requests_port)
main_socket.listen(1)


#Server socket
update_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
update_socket.bind(('localhost', update_listen_port))

print('Started Update Listening on port: ', update_listen_port)
update_socket.listen(len(workers))


#Saving mapper and reducer tasks
mapper_task = []
reducer_task = []

mapper_arr = []
mapper_done_task = []
mutex = Lock()

def schedule_worker(scheduler_type):
	global previously_selected
	selected_worker_id = -1
	#Critical Section ==========================================================================
	#Acquire Lock
	mutex.acquire()

	if(scheduler_type=='RANDOM'):
		random_list = list(range(len(workers)))
		random_index = random.randint(0,len(random_list)-1)

		while(len(random_list)>0 and workers[random_list[random_index]]['free_slots'] < 1):
			del random_list[random_index]
			if(len(random_list) < 1):
				random_index = -1
				break
			random_index = random.randint(0,len(random_list)-1)

		selected_worker_id = random_index

	elif(scheduler_type=='RR'):

		i = -1
		end = -1
		if(previously_selected==-1):
			for i in range(len(workers)):
				if(workers[i]['free_slots']>0):
					selected_worker_id = i
					break
		else:
			i=(previously_selected+1)%len(workers)
			end = previously_selected
			while(i!=end):
				if(workers[i]['free_slots']>0):
					selected_worker_id = i
					break
				i=(i+1)%len(workers)

		if(selected_worker_id!=-1):
			previously_selected = selected_worker_id


	elif(scheduler_type=='LL'):
		selected_worker_id = 0
		for i in range(1,len(workers)):
			if(workers[selected_worker_id]['free_slots'] < workers[i]['free_slots']):
				selected_worker_id = i
		if(workers[selected_worker_id]['free_slots']<1):
			selected_worker_id = -1

	#Release Lock
	mutex.release()
	#===============================================================================================

	return selected_worker_id


def schedule_mapper(scheduler_type):

	#Selecting Worker
	selected_worker_id = schedule_worker(scheduler_type)

	#Selecting Task
	if(selected_worker_id == -1):
		return 'wait'
	else:
		for j in range(len(mapper_task)):
			if('submitted' in mapper_task[j] and int(mapper_task[j]['submitted']) == 0):
				# Critical Section ================================================
				workers[selected_worker_id]['free_slots']-=1
				mapper_task[j]['worker_id'] = workers[selected_worker_id]['worker_id']
				mapper_task[j]['id'] = selected_worker_id
				mapper_task[j]['submitted'] = 1
				return json.dumps(mapper_task[j])
				# =================================================================
		return 'no_map'

def schedule_reducer(scheduler_type):

	#Selecting Worker
	selected_worker_id = schedule_worker(scheduler_type)

	#Selecting Task
	if(selected_worker_id == -1):
		return 'wait'
	else:
		for j in range(len(reducer_task)):
			if('submitted' in reducer_task[j] and int(reducer_task[j]['submitted'])==0):

				#Critical Section ===========================================================
				reducer_id = reducer_task[j]['task_id'].split('_')[0]

				for job in mapper_arr:
					temp_set = set(job[1:])
					if(len(temp_set)==1 and list(temp_set)[0]==1 and int(job[0])==int(reducer_id)):
						workers[selected_worker_id]['free_slots']-=1
						reducer_task[j]['worker_id'] = workers[selected_worker_id]['worker_id']
						reducer_task[j]['id'] = selected_worker_id
						reducer_task[j]['submitted'] = 1
						return json.dumps(reducer_task[j])
				#============================================================================
		return 'no_reduce'
def task_done(task_id):
    for i in range(len(mapper_task)):
        if(mapper_task[i]['task_id'] == task_id):
            mapper_task[i]['done']=1

def job_requests(job_q,joblist):


    while True:
        #Conection setup
        main_con, addr = main_socket.accept()
        jobs = main_con.recv(BUFF_SIZE)
        if(len(jobs)>0):
            jobs = json.loads(jobs)
            job_q.put(jobs)
            print(jobs)
            print(jobs['job_id'])
            joblist[jobs['job_id']] = [time.time(),len(jobs['reduce_tasks'])]



def job_assign(job_q,joblist):

    scheduler_type = scheduler
    result = ''
    outputfilename = "output"+scheduler_type+".txt"

    while True:
		# checks for jobs in job pool
        while(not job_q.empty()):
            job = job_q.get()
            #print(job['job_id'])

            temp_arr = [job['job_id']]
            temp_arr.extend([0]*len(job['map_tasks']))
            mapper_arr.append(temp_arr)

            #print(mapper_arr)

            for task in job['map_tasks']:
                task['done'] = 0
                task['submitted'] = 0
                mapper_task.append(task)

            for task in job['reduce_tasks']:
                task['done'] = 0
                task['submitted'] = 0
                reducer_task.append(task)

		# scheduling mapper
        result = schedule_mapper(scheduler_type)
        if(result == 'wait'):
            time.sleep(1)
        else:
            while(result != 'no_map'):
                if(result == 'wait'):
                    time.sleep(1)
                    #pass
                else:
                    result = json.loads(result)
                    dtask[result['task_id']] = time.time()
                    with open(outputfilename,"a") as f:
                        f.write("Sending "+ result['task_id'] + " at "+ str(time.time()) + " to "+ str(result['worker_id']) +  "\n")
                    print("Sending "+ result['task_id'] + " at "+ str(time.time()))


                    job_sockets[result['id']].send(json.dumps(result).encode())
                    job_sockets[result['id']].recv(100)
                result = schedule_mapper(scheduler_type)

def job_update(job_q,joblist):

    scheduler_type = scheduler
    result = ''
    outputfilename = "output"+scheduler_type+".txt"



    while True:
		#Server socket Connection accept
        update_conn, addr = update_socket.accept()
        update_task = update_conn.recv(BUFF_SIZE)
        if(len(update_task)>0):
            update_conn.send((' ').encode())
            print(update_task)

            # Critical Section =========================================================
            update_task = json.loads(update_task)
            #task
            #x = dtask[update_task['task_id']]
            #dtask[update_task['task_id']]= time.time()-x

            with open(outputfilename,"a") as f:

                f.write("Received " + str(update_task['task_id']) + " at " + str(time.time()) + " from " + str(update_task['worker_id']) + "\n")

            print("Received " + str(update_task['task_id'])+ " at " +str(time.time()) + " from " + str(update_task['worker_id']) )

            #jobs
            jid = update_task['task_id'].split("_")[0]
            #for i in joblist:
            if(update_task['task_id'].split("_")[1][0]=='R'):
                joblist[jid][1]-=1
                if(joblist[jid][1]==0):
                    t = joblist[jid][0]
                    t = time.time() - t
                    print(str(jid) + " ")
                    print(str(t) + "\n")
                    with open(outputfilename,"a") as f:

                        f.write(str(jid) + " ")
                        f.write(str(t) + "\n")

            task_done(update_task['task_id'])
            task_id = update_task['task_id'].split('_')
            if(task_id[1][0]=='M'):
                task_num = int(task_id[1][1:])

                #print(task_num,' tasknum------')
                for i in range(len(mapper_arr)):
                    if(mapper_arr[i] != None and int(mapper_arr[i][0])==int(task_id[0])):
                        mapper_arr[i][task_num+1]=1

            #print(mapper_arr)
            workers[int(update_task['id'])]['free_slots']+=1
            # ==========================================================================

        result = schedule_reducer(scheduler_type)
        if(result=='wait'):
            time.sleep(1)
            #pass
        else:
            while(result != 'no_reduce' and result != 'wait'):
                result = json.loads(result)
                #dtask[result['task_id']] = time.time()
                with open(outputfilename,"a") as f:
                    f.write("Sending "+ result['task_id'] + " at "+ str(time.time()) + " to "+ str(result['worker_id']) +   "\n")
                print("Sending "+ result['task_id'] + " at "+ str(time.time()) + " to "+ str(result['worker_id'])  + "\n")
                job_sockets[result['id']].send(json.dumps(result).encode())
                job_sockets[result['id']].recv(100)
                result = schedule_reducer(scheduler_type)

q = Queue()
joblist = dict()
main_thread = Thread(target = job_requests,args=(q,joblist ))
job_assign_thread = Thread(target = job_assign, args=(q,joblist ))
job_update_thread = Thread(target = job_update, args=(q,joblist ))

main_thread.start()
job_assign_thread.start()
job_update_thread.start()


# Main Thread waits for both the Threads to finish
#main_thread.join()
#job_assign_thread.join()
#job_update_thread.join()
#main_socket.close()
