import sys
import matplotlib.pyplot as plt

time1 = []
worker1 = []
countWorker1 = 0

time2 = []
worker2 = []
countWorker2 = 0

time3 = []
worker3 = []
countWorker3 = 0
with open("outputRR.txt", "r") as f:
    line = f.readline().strip('\n').split(" ")
    initialTime = float(line[3])

with open("outputRR.txt", "r") as f:

    for lines in f.readlines():
        line = lines.strip('\n').split(" ")

        # if(line[0]=='Sending' and line[1][0]=='0' and line[1][3]=='0'):
        #     initialTime = float(line[3])

        if(len(line)>2):
            #worker1
            if(line[5]=='1'):
                if(line[0]=='Sending'):
                    countWorker1+=1
                    time1.append(float(line[3])-initialTime)
                    worker1.append(countWorker1)
                    #print(float(line[3]), countWorker1)
                elif(line[0]=='Received'):
                    countWorker1-=1
                    time1.append(float(line[3])-initialTime)
                    worker1.append(countWorker1)
                    #print(float(line[3]), countWorker1)
            elif(line[5]=='2'):
                if(line[0]=='Sending'):
                    countWorker2+=1
                    time2.append(float(line[3])-initialTime)
                    worker2.append(countWorker1)
                    #print(float(line[3]), countWorker1)
                elif(line[0]=='Received'):
                    countWorker2-=1
                    time2.append(float(line[3])-initialTime)
                    worker2.append(countWorker1)
                    #print(float(line[3]), countWorker1)
            elif(line[5]=='3'):
                if(line[0]=='Sending'):
                    countWorker3+=1
                    time3.append(float(line[3])-initialTime)
                    worker3.append(countWorker1)
                    #print(float(line[3]), countWorker1)
                elif(line[0]=='Received'):
                    countWorker3-=1
                    time3.append(float(line[3])-initialTime)
                    worker3.append(countWorker1)
                    #print(float(line[3]), countWorker1)
#print(time1)
#print(worker1)
plt.xlabel('Time (sec)')
plt.ylabel('Number of tasks running')
plt.scatter(time1, worker1,color='blue')
plt.title("worker1")
plt.show()
plt.scatter(time2, worker2,color ="red")
plt.xlabel('Time (sec)')
plt.ylabel('Number of tasks running')
plt.title("worker2")
plt.show()
plt.scatter(time3, worker3,color='green')
plt.xlabel('Time (sec)')
plt.ylabel('Number of tasks running')
plt.title("worker3")
plt.show()

dtask = dict()
jobs = []
tasks = []
with open("outputRR.txt", "r") as f:
    for lines in f.readlines():
        l = lines.split(" ")
        if(len(l)==2):
            jobs.append(float(l[1].strip()))
        elif(l[0]=="Sending"):
            dtask[l[1]] = float(l[3].strip())
        elif(l[0]=="Received"):
            dtask[l[1]] = float(l[3].strip())-dtask[l[1]]
for i in dtask:
    tasks.append(dtask[i])
tasks = sorted(tasks)
jobs = sorted(jobs)
print("Average time taken by tasks : ",sum(tasks)/len(tasks))
if(len(tasks)%2==0):
    print("Median time taken by tasks : ",tasks[int(len(tasks)/2)])
else:
    print("Median time taken by tasks : ",tasks[int((len(tasks)+1)/2)])

print("Average time taken by jobs : ",sum(jobs)/len(jobs))
if(len(tasks)%2==0):
    print("Median time taken by jobs: ",jobs[int(len(jobs)/2)])
else:
    print("Median time taken by jobs: ",jobs[int((len(jobs)+1)/2)])
