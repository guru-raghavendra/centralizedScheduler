import sys
import matplotlib.pyplot as plt


LLfilename = "outputLL.txt"
RRfilename = "outputRR.txt"
RANDOMfilename = "outputRANDOM.txt"
tasks = []
jobs = []

########Least Loaded###############

with open(LLfilename,"r") as f:
    for lines in f.readlines():
        if(len(lines.split(" "))==1):
            continue
        elif('M' in lines or 'R' in lines):
            lines = lines.split(" ")
            tasktime = float(lines[1])
            tasks.append(tasktime)
        else:
            lines = lines.split(" ")
            jobtime = float(lines[1])
            jobs.append(jobtime)
ntasks = tasks

tasks = sorted(tasks)
jobs = sorted(jobs)

if(len(tasks)%2==0):
    print("Median time taken by tasks in LL ",tasks[int(len(tasks)/2)])
else:
    print("Median time taken by tasks in LL",tasks[int((len(tasks)+1)/2)])


if(len(jobs)%2==0):
    print("Median time taken by jobs in LL",jobs[int(len(jobs)/2)])
else:
    print("Median time taken by jobs LL",jobs[int((len(jobs)+1)/2)])

print("Average time taken by tasks LL",sum(tasks)/len(tasks))
print("Average time taken by jobs LL",sum(jobs)/len(jobs))

plot1 = plt.figure("Least Loaded")
plt.plot(list(range(1,len(ntasks)+1)), ntasks)
plt.xlabel('Tasks')
plt.ylabel('Time(sec)')
plt.title('Least Loaded')
plt.show()


tasks = []
jobs = []

with open(RRfilename,"r") as f:
    for lines in f.readlines():
        if(len(lines.split(" "))==1):
            continue
        elif('M' in lines or 'R' in lines):
            lines = lines.split(" ")
            tasktime = float(lines[1])
            tasks.append(tasktime)
        else:
            lines = lines.split(" ")
            jobtime = float(lines[1])
            jobs.append(jobtime)
ntasks = tasks
tasks = sorted(tasks)
jobs = sorted(jobs)


if(len(tasks)%2==0):
    print("Median time taken by tasks  in RR",tasks[int(len(tasks)/2)])
else:
    print("Median time taken by tasks in RR",tasks[int((len(tasks)+1)/2)])


if(len(jobs)%2==0):
    print("Median time taken by jobs RR",jobs[int(len(jobs)/2)])
else:
    print("Median time taken by jobs RR",jobs[int((len(jobs)+1)/2)])

print("Average time taken by tasks RR",sum(tasks)/len(tasks))
print("Average time taken by jobs RR",sum(jobs)/len(jobs))

plot2 = plt.figure("Round Robin")
plt.plot(list(range(1,len(ntasks)+1)), ntasks)
plt.xlabel('Tasks')
plt.ylabel('Time(sec)')
plt.title('RoundRobin')
plt.show()

tasks = []
jobs = []

with open(RANDOMfilename,"r") as f:
    for lines in f.readlines():
        if(len(lines.split(" "))==1):
            continue
        elif('M' in lines or 'R' in lines):
            lines = lines.split(" ")
            tasktime = float(lines[1])
            tasks.append(tasktime)
        else:
            lines = lines.split(" ")
            jobtime = float(lines[1])
            jobs.append(jobtime)
ntasks = tasks

tasks = sorted(tasks)
jobs = sorted(jobs)




if(len(tasks)%2==0):
    print("Median time taken by tasks in RANDOM ",tasks[int(len(tasks)/2)])
else:
    print("Median time taken by tasks in RANDOM",tasks[int((len(tasks)+1)/2)])


if(len(jobs)%2==0):
    print("Median time taken by jobs in RANDOM",jobs[int(len(jobs)/2)])
else:
    print("Median time taken by jobs in RANDOM",jobs[int((len(jobs)+1)/2)])

print("Average time taken by tasks in RANDOM",sum(tasks)/len(tasks))
print("Average time taken by jobs in RANDOM ",sum(jobs)/len(jobs))

plot3 = plt.figure("RANDOM")
plt.plot(list(range(1,len(ntasks)+1)), ntasks)
plt.xlabel('Tasks')
plt.ylabel('Time(sec)')
plt.title('RANDOM')
plt.show()
