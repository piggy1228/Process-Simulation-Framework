'''
CSCI-4210 Operating System
Project 1
Zhilin Han and Kexin Zhu

The program take a input filename and output filename.
Then, simulate the CPU scheduling for the processes given
in the input file.
The key step of the simulation will be printed through
stdout, and the summary of the statistics will be stored in
a text file with the given output filename.
FCFS, SRT and RR will be simulated.

'''
from queue import PriorityQueue
import collections
from collections import defaultdict
import copy
import sys


def multi_level_dict():
    return defaultdict(multi_level_dict)

'''
create a class to record each process
'''
class Process:
    def __init__(self, id, arrival_time, burst_time, num_burst, io_time):
        self.id = id
        self.arrival_time = int(arrival_time)
        self.burst_time = int(burst_time)
        self.origin_numburst = int(num_burst)
        self.num_burst = int(num_burst)
        self.io_time = int(io_time)
        self.func = 'none'
        self.start_time = 0
        self.end_time = 0
        self.final_time = 0
        self.first_start_time = int(arrival_time)
        self.original_burst_time = int(burst_time)
        self.actual_burst_time = 0
        self.sch_burst_time = 0
        self.last_burst_time = 0

    def __lt__(self, other):
        if(self.func=='none' or self.func=='FCFS' or self.func=='RR'):
            self_priority=self.arrival_time
            other_priority=other.arrival_time
            self_sec_pro=self.id
            other_sec_pro=other.id
        else:# when it's SRT
            self_priority=self.burst_time
            other_priority=other.burst_time
            self_sec_pro = self.id
            other_sec_pro = other.id

        if(self_priority<other_priority):
            return True
        elif self_priority==other_priority:
            return self_sec_pro<other_sec_pro
        else:
            return False

    def time_left(self):
        return self.original_burst_time - self.actual_burst_time



'''
this is a helper function used to get the string output of queue
'''
def getq(readyq):
    res=[]
    while not readyq.empty():
        x=readyq.get()
        res.append(x)
    if(len(res)==0):
        return '[Q <empty>]'
    else:
        a='[Q'
        for i in res:
            a+=' '+str(i.id)
            readyq.put(i)
        a+=']'
        return a



'''
This is a helper function used to output a FCFS timeline, which is a dictionary.
'''
def cutq(final,time,id,status):
    res=[]
    if(status=='end'):
        for i in final:
            if(i.arrival_time<=time and i.start_time>time):
                if(i.arrival_time==time and i.id!=id):
                    break
                res.append(i.id)
            if(i.arrival_time==time and i.id==id):
                break
    else:
        for i in final:
            if(i.arrival_time<=time and i.start_time>time):
                res.append(i.id)
                if(i.end_time==time and i.id==id):
                    break
            if(i.arrival_time==time and i.id==id):
                break
    if(len(res)==0):
        return '[Q <empty>]'
    else:
        a='[Q'
        for i in res:
            a+=' '+str(i)
        a+=']'
    return a

'''
According to the current time, seperate the stage of the current process running state into 3 stages: 4 ms switch in,
CPU_burst_time, 4 ms switch out. In evert stage, to compare the current running process remaining time, and the arriving
other processes CPU burst time, if in the second stage, then preempt the current running process. Otherwise, just remove
the process from the block queue and add into ready queue
'''
def SRT(process_list):
    print("time 0ms: Simulator started for SRT [Q <empty>]")
    #initialization
    preemption_num = 0
    context_switch = 0
    ready_queue = PriorityQueue()
    ready = []

    blocked = []
    currtime = 0
    for i in process_list:
        i.func='SRT'
        ready.append(i)
        ready.sort(key=lambda x:x.arrival_time)
        if i.arrival_time == currtime:
            ready_queue.put(i)
            strQ = getq(ready_queue)
            print("time 0ms: Process {} arrived and added to ready queue {}".format(i.id,strQ))
        else:
            blocked.append(i)


    if ready_queue.empty():
        #blocked = copy.deepcopy(process_list)
        current = ready.pop(0)
        ready.sort(key=lambda x:x.arrival_time)
        blocked.sort(key=lambda x: x.arrival_time)
        blocked.remove(current)
        currtime = current.arrival_time
        ready_queue.put(current)
        strQ = getq(ready_queue)
        if current.origin_numburst == current.num_burst:
            print("time {}ms: Process {} arrived and added to ready queue {}".format(current.arrival_time,
                                                                                     current.id, strQ))
        else:
            print("time {}ms: Process {} completed I/O; added to ready queue {}".format(
                current.arrival_time, current.id, strQ))

    #go into the loop until all the process are terminate
    while not (ready_queue.empty() and len(blocked) == 0):
        context_switch += 1
        if ready_queue.empty():
            blocked.sort(key=lambda x: x.arrival_time)
            currtime = blocked[0].arrival_time
            ready_queue.put(blocked[0])
            blocked_process = blocked[0]
            blocked.remove(blocked[0])
            strQ = getq(ready_queue)

            if blocked_process.origin_numburst == blocked_process.num_burst:
                print("time {}ms: Process {} arrived and added to ready queue {}".format(blocked_process.arrival_time,
                                                                                         blocked_process.id, strQ))
            else:
                print("time {}ms: Process {} completed I/O; added to ready queue {}".format(
                    blocked_process.arrival_time, blocked_process.id, strQ))

        current = ready_queue.get()

        ####check if any io completeed in the current 4 time range,just add the blocked process back to theready queue
        for i in range(len(blocked)):
            blocked_process = blocked[0]
            if blocked_process.arrival_time < currtime+4:
                blocked.remove(blocked_process)
                ready_queue.put(blocked_process)
                strQ = getq(ready_queue)
                if blocked_process.origin_numburst == blocked_process.num_burst:
                    print(
                        "time {}ms: Process {} arrived and added to ready queue {}".format(blocked_process.arrival_time,
                                                                                           blocked_process.id, strQ))
                else:
                    print("time {}ms: Process {} completed I/O; added to ready queue {}".format(
                        blocked_process.arrival_time, blocked_process.id, strQ))


        currtime += 4
        current.start_time = currtime
        strQ = getq(ready_queue)
        if current.burst_time == current.original_burst_time:
            print("time {}ms: Process {} started using the CPU {}".format(currtime,current.id,strQ))
        else:
            print("time {}ms: Process {} started using the CPU with {}ms remaining {}".format(currtime, current.id,current.burst_time ,strQ))


        #######each time compare the cpu time and the new arrival process from blocked
        current_terminate_time = currtime + current.burst_time
        blocked.sort(key=lambda x: x.arrival_time)
        preempted = 0
        for i in range(len(blocked)):
            blocked_process = blocked[0]
            if blocked_process.arrival_time < current_terminate_time:
                # preempt
                if blocked_process.burst_time < (current_terminate_time - blocked_process.arrival_time - 8):
                    blocked.remove(blocked_process)
                    ready_queue.put(blocked_process)
                    currtime = blocked_process.arrival_time
                    # update the new cpu burst time
                    current.burst_time = current.burst_time - (currtime - current.start_time)
                    # update arrival_time
                    current.arrival_time = currtime

                    # put the current back into the ready queue
                    ready_queue.put(current)

                    preempted = 1

                    if blocked_process.origin_numburst == blocked_process.num_burst:
                        print(
                            "time {}ms: Process {} arrived and will preempt {} {}".format(currtime, blocked_process.id,
                                                                                          current.id, strQ))
                    else:
                        print("time {}ms: Process {} completed I/O and will preempt {} {}".format(currtime,
                                                                                                  blocked_process.id,
                                                                                                  current.id, strQ))
                    break
                blocked.remove(blocked_process)
                ready_queue.put(blocked_process)
                strQ = getq(ready_queue)
                if blocked_process.origin_numburst == blocked_process.num_burst:
                    print(
                        "time {}ms: Process {} arrived and added to ready queue {}".format(blocked_process.arrival_time,
                                                                                           blocked_process.id, strQ))
                else:
                    print("time {}ms: Process {} completed I/O; added to ready queue {}".format(
                        blocked_process.arrival_time, blocked_process.id, strQ))

        # when there is preemption
        if preempted == 1:
            ##update current time
            currtime += 4
            preemption_num += 1
            continue



        ##when the process is not preempted
        currtime += current.burst_time

        blocked.sort(key=lambda x: x.id)
        #terminate the current process
        if(current.num_burst==1):
            strQ = getq(ready_queue)
            print("time {}ms: Process {} terminated {}".format(currtime,current.id,strQ))
            current.final_time = currtime + 4
        #when there is still burst num left, go into blocked list
        #complete burst
        else:
            blocked.append(current)
            strQ = getq(ready_queue)
            if(current.num_burst > 2):
                print("time {}ms: Process {} completed a CPU burst; {} bursts to go {}".format(currtime,current.id,current.num_burst-1,strQ))
            else:
                print("time {}ms: Process {} completed a CPU burst; {} burst to go {}".format(currtime, current.id,current.num_burst - 1,strQ))
            print("time {}ms: Process {} switching out of CPU; will block on I/O until time {}ms {}".format(currtime,current.id,currtime+current.io_time+4,strQ))


        ####there will have 4 s time to complete block
        for i in range(len(blocked)):
            blocked_process = blocked[0]
            if currtime <=blocked_process.arrival_time < currtime+4 :
                blocked.remove(blocked_process)
                ready_queue.put(blocked_process)
                strQ = getq(ready_queue)
                if not blocked_process.origin_numburst == blocked_process.num_burst:

                    print("time {}ms: Process {} completed I/O; added to ready queue {}".format(
                        blocked_process.arrival_time, blocked_process.id, strQ))
                else:

                    print("time {}ms: Process {} arrived and added to ready queue {}".format(blocked_process.arrival_time, blocked_process.id, strQ))

        currtime += 4

        ##update current num_burst, arrival_time,
        current.burst_time = current.original_burst_time
        current.num_burst -= 1
        current.arrival_time = currtime + current.io_time
    print("time {}ms: Simulator ended for SRT".format(currtime))
    return context_switch,preemption_num


'''
Robin Round algorithm:
using 4 PriorityQueue to update the states
time queue keeps the time of interst when something's gonna happen
ready queue keeps the processes that have already arrived
blocked queue keeps the process that blocks on IO
allp queue keeps all the process that haven't ended
'''
def RR(process_list,ts):
    print("time 0ms: Simulator started for RR [Q <empty>]")
    time=PriorityQueue()
    ready=PriorityQueue()
    blocked=PriorityQueue()
    allp=PriorityQueue()
    run=[None]
    num_cs=0
    p_list=copy.deepcopy(process_list)
    for i in process_list:
        i.func='RR'
        allp.put(i)
        time.put(i.arrival_time)
    currtime=0
    preempt=0
    end=[]
    mark_switch=-1
    while True:
        if mark_switch==currtime:
            mark_switch=-1
            num_cs+=1
            if run[0] is not None:
                if currtime+run[0].sch_burst_time not in time.queue:
                    time.put(currtime+run[0].sch_burst_time)
                run[0].sch_burst_time=0
                if (run[0].actual_burst_time==0):
                    print("time {}ms: Process {} started using the CPU {}".format(currtime, run[0].id, getq(ready)))
                else:
                    print("time {}ms: Process {} started using the CPU with {}ms remaining {}".format(currtime, run[0].id,run[0].time_left(), getq(ready)))
        #check process terminate or complete burst
        if (run[0] is not None and run[0].actual_burst_time==run[0].original_burst_time):
            run[0].num_burst-=1
            if(run[0].num_burst==0):
                run[0].final_time=currtime+4
                end.append(copy.deepcopy(run[0]))
                print("time {}ms: Process {} terminated {}".format(currtime,run[0].id,getq(ready)))
            else:
                if run[0].num_burst>1:
                    print("time {}ms: Process {} completed a CPU burst; {} bursts to go {}" .format(currtime, run[0].id, run[0].num_burst, getq(ready)))
                else:
                    print ("time {}ms: Process {} completed a CPU burst; {} burst to go {}" .format(currtime, run[0].id, run[0].num_burst, getq(ready)))
                end_t=currtime + run[0].io_time+4
                run[0].arrival_time=end_t
                run[0].actual_burst_time=0
                run[0].last_burst_time=0
                blocked.put(copy.deepcopy(run[0]))
                if end_t not in time.queue:
                    time.put(end_t)
                print ("time {}ms: Process {} switching out of CPU; will block on I/O until time {}ms {}".format(currtime, run[0].id, end_t, getq(ready)))
            run[0]=None
            mark_switch=currtime+4
            if(mark_switch not in time.queue):
                time.put(mark_switch)
        #check if time slice expires
        last_proc=copy.deepcopy(run[0])
        if mark_switch==-1:
            if run[0] is None and not ready.empty():
                run[0] = ready.get()
                run[0].sch_burst_time = min(ts, run[0].time_left())
            elif run[0] is not None and run[0].actual_burst_time != \
                    run[0].last_burst_time and (run[0].actual_burst_time \
                    % ts) == 0:
                if ready.empty():
                    run[0].sch_burst_time = min(ts, run[0].time_left())
                    print("time {}ms: Time slice expired; no preemption because ready queue is empty {}".format(currtime,getq(ready)))
                else:
                    run[0].last_burst_time = run[0].actual_burst_time
                    preempt += 1
                    tmp=copy.deepcopy(run[0])
                    print( "time {}ms: Time slice expired; process {} preempted with {}ms to go {}" .format(currtime,tmp.id, tmp.time_left(),getq(ready)))
                    run[0].arrival_time=currtime
                    ready.put(copy.deepcopy(run[0]))
                    run[0] = None
            if(last_proc is None and run[0] is None):
                pass
            elif(run[0] is None or last_proc is None):
                mark_switch=currtime+4
                if mark_switch not in time.queue:
                    time.put(mark_switch)
            elif run[0].id !=last_proc.id:
                mark_switch=currtime+4
                if mark_switch not in time.queue:
                    time.put(mark_switch)
            elif run[0] is not None and run[0].sch_burst_time!=0:
                if (currtime+run[0].sch_burst_time) not in time.queue:
                    time.put(currtime+run[0].sch_burst_time)
                run[0].sch_burst_time=0
        #check back from block queue
        while not blocked.empty():
            it=blocked.get()
            if(it.arrival_time==currtime):
                ready.put(it)
                print ("time {}ms: Process {} completed I/O; added to ready queue {}" .format (currtime, it.id,
                    getq(ready)))
            else:
                blocked.put(it)
                break
        #check new arrival process
        while not allp.empty():
            it=allp.get()
            if it.first_start_time==currtime:
                ready.put(copy.deepcopy(it))
                print("time {}ms: Process {} arrived and added to ready queue {}".format(currtime, it.id,
                    getq(ready)))
            else:
                allp.put(it)
                break

        last_proc=copy.deepcopy(run[0])
        if mark_switch==-1:
            if run[0] is None and not ready.empty():
                run[0] = ready.get()
                run[0].sch_burst_time = min(ts, run[0].time_left())

            if(last_proc is None and run[0] is None):
                pass
            elif(run[0] is None or last_proc is None):
                mark_switch=currtime+4
                if mark_switch not in time.queue:
                    time.put(mark_switch)
            elif run[0].id !=last_proc.id:
                mark_switch=currtime+4
                if mark_switch not in time.queue:
                    time.put(mark_switch)
            elif run[0] is not None and run[0].sch_burst_time!=0:
                if (currtime+run[0].sch_burst_time) not in time.queue:
                    time.put(currtime+run[0].sch_burst_time)
                run[0].sch_burst_time=0
        if(time.empty()):
            break
        timegap=time.get()-currtime
        if mark_switch==-1 and run[0] is not None:
            run[0].actual_burst_time+=timegap
        currtime+=timegap
    print("time {}ms: Simulator ended for RR".format(currtime))
    return num_cs//2,preempt,end



'''
Output the time line get from FCFS function, according to the time_line schedule, each time one status happens,
print the output out
'''
def output_simulation(time_line,final, func):
    print("time 0ms: Simulator started for", func ,"[Q <empty>]")
    # to regulate the order of the output
    status_list = ["completeburst","out","backio","terminate","arrival","start",]
    for t in time_line:
        for status in status_list:
            if  (status == "arrival" and "arrival" in  time_line[t]):
                for p in time_line[t][status]:
                    if "backio" in time_line[t]:
                        for p2 in time_line[t]["backio"]:
                            if p.id != p2.id:
                                print("time", "{}ms: Process {} arrived and added to ready queue {}".format(int(t), p.id,
                                    cutq(final, t, p.id,'arrival')))

                    else:
                        print("time","{}ms: Process {} arrived and added to ready queue {}".format(int(t), p.id,
                            cutq(final, t, p.id,'arrival')))

            ##for 5 status in the time line
            if ((status == "start") and ("start" in  time_line[t])):
                print("time", "{}ms: Process {} started using the CPU {}".format(int(t),time_line[t][status].id,cutq(final,t,time_line[t][status].id,'start')))
            if (status == "completeburst" and "completeburst" in time_line[t]):
                if time_line[t][status].num_burst >2:
                    print("time {}ms: Process {} completed a CPU burst; {} bursts to go {}".format(int(t),time_line[t][status].id,time_line[t][status].num_burst-1, cutq(final,t,time_line[t][status].id,'end')))
                else:
                    print("time {}ms: Process {} completed a CPU burst; {} burst to go {}".format(int(t), time_line[t][
                        status].id, time_line[t][status].num_burst - 1, cutq(final, t, time_line[t][status].id, 'end')))
            if  (status == "out" and "out" in time_line[t]):
                 print("time {}ms: Process {} switching out of CPU; will block on I/O until time {}ms {}".format(t,
                        time_line[t][status].id,time_line[t][status].io_time+t+4, cutq(final,t,time_line[t][status].id,'end')))

            if (status == "backio" and "backio" in time_line[t]):
                for p in time_line[t][status]:
                    print("time {}ms: Process {} completed I/O; added to ready queue {}".format(t, p.id, cutq(final,t,p.id,'arrival')))

            if (status == "terminate" and  "terminate" in time_line[t]):
                print("time {}ms: Process {} terminated {}".format(int(t), time_line[t][status].id,cutq(final,t,time_line[t][status].id,'end')))
    print("time {}ms: Simulator ended for {}".format(int(t+4),func))



'''
get all the process in the ready queue, each time get one process, set its arrival time, finish time in the time line
schedule
'''

def FCFS(process_list):
    context_switch = 0
    ready = PriorityQueue()
    time_line=multi_level_dict()
    for i in process_list:
        i.func='FCFS'
        ready.put(i)
        if "arrival" in time_line[i.arrival_time]:
            time_line[i.arrival_time]['arrival'].append(copy.deepcopy(i))
        if "arrival" not in time_line[i.arrival_time]:
            time_line[i.arrival_time]['arrival']=[copy.deepcopy(i)]

    currtime=0
    #create a final_queue to get the current time queue
    final_queue=[]
    while(not ready.empty()):
        context_switch += 1
        current=ready.get()

        if not time_line:
            currtime=0
        elif (currtime<current.arrival_time):
            currtime=current.arrival_time
        if (current.num_burst!=current.origin_numburst):
            if "arrival" in time_line[current.arrival_time]:
                time_line[current.arrival_time]['arrival'].append(copy.deepcopy(current))

            if "arrival" not in time_line[current.arrival_time]:
                time_line[current.arrival_time]['arrival'] = [copy.deepcopy(current)]

        current.start_time=currtime+4
        time_line[currtime+4]['start']=copy.deepcopy(current)

        currtime+=current.burst_time+4
        current.end_time=currtime
        if(current.num_burst==1):
            current.final_time = currtime + 4
            time_line[currtime]['terminate']=copy.deepcopy(current)

        else:
            time_line[currtime]['out']=copy.deepcopy(current)
            time_line[currtime]['completeburst']=copy.deepcopy(current)

        currtime+=4
        final_queue.append(copy.deepcopy(current))
        current.num_burst-=1
        if(current.num_burst>0):
            current.arrival_time=currtime+current.io_time
            ready.put(current)
            if "backio" in time_line[currtime+current.io_time]:
                time_line[currtime+current.io_time]['backio'].append(copy.deepcopy(current))
            if 'backio' not in time_line[currtime+current.io_time]:
                time_line[currtime+current.io_time]['backio']=[copy.deepcopy(current)]
    return time_line,final_queue,context_switch





'''
calculate the average cpu burst time, average wait time and turnaround time
'''
def output_file(func,process_list, context_switch, preemption_num):
    ##calculate the average burst time
    burst_time = 0
    process_num = 0
    for process in process_list:
        burst_time += process.origin_numburst * process.original_burst_time
        process_num += process.origin_numburst
    average_burst_time = burst_time / process_num

    # TO calculate wait time
    if func !="RR":
        wait_time = 0
        for process in process_list:
            wait_time += process.final_time - process.first_start_time - process.origin_numburst * process.original_burst_time - (process.origin_numburst - 1) * process.io_time
        wait_time -= (8 * context_switch + 4 * preemption_num)
        average_wait_time = wait_time / process_num
    else:
        wait_time=0
        for process in process_list:
            wait_time+=process.final_time-process.first_start_time-process.origin_numburst*process.original_burst_time-(process.origin_numburst-1)*process.io_time
        wait_time-=8*context_switch
        average_wait_time = wait_time / process_num

    #different method to calculate the turnaround time
    if func == "SRT":
        turnaround_time = burst_time + wait_time + 4 * (2 * context_switch + preemption_num)
    elif func=="FCFS":
        turnaround_time = burst_time + wait_time + 4 * (2 * context_switch -preemption_num)
    else:
        turnaround_time = burst_time + wait_time + 8 * context_switch

    average_turnaround_time = turnaround_time / process_num


    #get the result to output file
    out_str = "Algorithm %s\n"%func
    out_str += "-- average CPU burst time: %3.2f ms\n"%average_burst_time
    out_str += "-- average wait time: %3.2f ms\n" % average_wait_time
    out_str += "-- average turnaround time: %3.2f ms\n" % average_turnaround_time
    out_str += "-- total number of context switches: " + str(context_switch) + "\n"
    out_str += "-- total number of preemptions: " + str(preemption_num) + "\n"
    return out_str


if __name__ == "__main__":
    # Argument input error handling
    if len(sys.argv) < 3:
        sys.stderr.write("ERROR: Invalid arguments\n")
        sys.stderr.write("USAGE: ./main.py <input-file> <output-file>\n")
        sys.exit()
    # Read the input and output filename
    input_file_str = sys.argv[-2]
    output_file_str = sys.argv[-1]
    process_list=[]
    f=open(input_file_str,'r')
    for line in f:
        if(line[0]=='#' or line[0]==' ' or line[0]=='\n'):
            pass
        else:
            process_info=line.split('|')
            if(len(process_info)!=5):
                sys.stderr.write("ERROR: Invalid input file format\n")
                sys.exit()
            process_list.append(Process(process_info[0], process_info[1],
                                  process_info[2], process_info[3],
                                  process_info[4]))


    process_list1 = copy.deepcopy(process_list)
    process_list2 = copy.deepcopy(process_list)
    process_list3 = copy.deepcopy(process_list)
    ###FCFS
    a,b,context_switch=FCFS(process_list1)
    od = collections.OrderedDict(sorted(a.items()))
    output_simulation(od,b, "FCFS")
    FCFS_result = output_file("FCFS", process_list1,context_switch,0)

    ###SRT
    print("")
    context_switch,preemption_num = SRT(process_list2)
    SRT_result = output_file("SRT", process_list2, context_switch,preemption_num)

    ###RR
    print("")
    context_switch, preemption_num,final_RR_list = RR(process_list3, 70)
    RR_result = output_file("RR", final_RR_list, context_switch, preemption_num)


    #wirte to the test file
    # Write simulation results to file
    out_f = open(output_file_str, "w+")
    out_f.write(FCFS_result)
    out_f.write(SRT_result)
    out_f.write(RR_result)
    out_f.close()
