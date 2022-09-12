from kubernetes import config
import kubernetes.client
import requests
import time
import socket
import gzip
import shutil
import logging
from aiohttp import ClientSession
import asyncio
from scipy.stats import skew
import numpy as np


timeout_seconds = 30
maindict = {}
timesdict = {}
helplist = []
checklist = []
lastmaindict = {}
avgdict = {}

listnodeload1 = []
listmemory = []
listnodediskionow=[]
listnode_network_receive_bytes_total = []
list0 = []
list1 = []
list2 = []
list3 = []
list4 = []
list5 = []
list6 = []
list7 = []
list8 = []
list9 = []
list10 = []

logging.basicConfig(level=logging.INFO)

def timewriter(text):
    try:
        f = open("exectime", 'a')
    except:
        print("Open exectime failed")
    try:
        f.write(text)
        f.write("\n")
        f.close
    except:
        print("Write error")

def errorwriter(text):
    try:
        f = open("errormetrics", 'a')
    except:
        print("Open exectime failed")
    try:
        f.write(text)
        f.write("\n")
        f.close
    except:
        print("Write error")

def errorpernode(timestamp,name,std,mean,skewness,list):
    try:
        f = open("error.csv", 'a')
    except:
        print("Open exectime failed")
    try:
        f.write(str(timestamp)+","+str(name)+","+str(std)+","+str(mean)+","+str(std/mean)+","+str(skewness)+","+str(list))
        f.write("\n")
        f.close
    except:
        print("Write error")

def getControllerMasterIP():
    config.load_kube_config()
    api_instance = kubernetes.client.CoreV1Api()
    master_ip = ""

    try:
        nodes = api_instance.list_node(
            pretty=True, _request_timeout=timeout_seconds)
        nodes = [node for node in nodes.items if
                'node-role.kubernetes.io/master' in node.metadata.labels]
        addresses = nodes[0].status.addresses
        master_ip = [i.address for i in addresses if i.type == "InternalIP"][0]
    except:
        print("Connection timeout after " +
            str(timeout_seconds) + " seconds to host cluster")

    return master_ip

def parseforsetkeys(textline):
    parseddata = textline.split("{")
    smalldata = parseddata[0].split("_")
    return set(smalldata)

def parseforsethelp(textline):
    parseddata = textline.split(" ")
    smalldata = parseddata[2].split("_")
    return set(smalldata)

def parsefortstrkey(textline):
    parseddata = textline.split("{")
    return str(parseddata[0])

def parseforstrhelp(textline):
    origdata = textline.strip('\n')
    parseddata = origdata.split(" ")
    return str(parseddata[2])

def parseforstrhelpANDtype(textline):
    origdata = textline.strip('\n')
    parseddata = origdata.split(" ")
    return str(parseddata[3]), str(parseddata[2])

def parsevalue(textline):
    origdata = textline.strip('\n')
    if "}" in origdata:
        firstparse = origdata.split("}")
        parseddata = firstparse[1].split(" ")
    else:
        parseddata = origdata.split(" ")

    return str(parseddata[1])

def parsename(textline):
    origdata = textline.strip('\n')
    if "}" in origdata:
        firstparse = origdata.split("}")
        parseddata = firstparse[0] + "}"
    else:
        firstparse = origdata.split(" ")
        parseddata = firstparse[0]

    return str(parseddata)

def gettargets(prom_host):
    start = time.perf_counter()
    prom_port = 30090
    prom_url = "http://" + str(prom_host) + ":" + \
                            str(prom_port) + "/api/v1/targets"
    prom_header = {'Accept-Encoding': 'gzip'}
    r = requests.get(url=prom_url,headers=prom_header)
    data = r.json()
    scrapeurl = []
    for item in data["data"]["activeTargets"]:
        if item["labels"]["job"] == "node-exporter":
            if item["labels"]["instance"] != "10.158.4.2:9100":
                scrapeurl.append(item["scrapeUrl"])
    end = time.perf_counter()
    timewriter("gettargets" + " " + str(end-start))
    return scrapeurl

async def fetch(link, session,number):
    try:
        async with session.get(link) as response:
            html_body = await response.text()
            fname = "before" + str(number)
            f = open(fname, 'w')
            f.write(html_body)
            f.close
    except:
        print("get metrics failed")

async def asyncgetmetrics(links):
    async with ClientSession() as session:
        tasks = [asyncio.create_task(fetch(link, session, links.index(link))) for link in links]
        await asyncio.gather(*tasks)

def merge(path):
    start = time.perf_counter()
    f = open(path, 'r')
    global maindict
    global timesdict
    global checklist
    global listnodeload1
    global listmemory
    counterformetrics = 1
    valuelist = []
    metricsname = []
    tempdict = {}
    helplistappend = helplist.append
    checklistappend = checklist.append
    valuelistappend = valuelist.append
    metricsnameappend = metricsname.append
    for line in f.readlines():
        if line[0] == "#":
            if counterformetrics % 2 == 0:
                if line not in helplist:
                    helplistappend(line)
                    checklistappend(parseforstrhelp(line))
            counterformetrics += 1
        else:
            valuelistappend(parsevalue(line))
            metricsnameappend(parsename(line))

    if not maindict:
        maindict = dict(zip(metricsname, valuelist))
        for k in maindict.keys():
            if k == "node_load1":
                listnodeload1.append(float(maindict[k]))
            if k == "node_load15":
                list6.append(float(maindict[k]))
            if k == "node_load5":
                list7.append(float(maindict[k]))
            if k == "node_memory_MemFree_bytes":
                listmemory.append(float(maindict[k]))
            if k == "node_disk_io_now{device=\"vda\"}":
                listnodediskionow.append(float(maindict[k]))
            if k == "node_memory_MemAvailable_bytes":
                list8.append(float(maindict[k]))
            if k == "node_network_transmit_bytes_total{device=\"ens3\"}":
                list0.append(float(maindict[k]))
            if k == "node_sockstat_TCP_alloc":
                list1.append(float(maindict[k]))
            if k == "node_cpu_seconds_total{cpu=\"1\",mode=\"idle\"}":
                list2.append(float(maindict[k]))
            if k == "node_cpu_seconds_total{cpu=\"0\",mode=\"system\"}":
                list3.append(float(maindict[k]))
            if k == "node_cpu_seconds_total{cpu=\"1\",mode=\"system\"}":
                list4.append(float(maindict[k]))
            if k == "node_cpu_seconds_total{cpu=\"0\",mode=\"idle\"}":
                list5.append(float(maindict[k]))
            if k == "node_cpu_seconds_total{cpu=\"0\",mode=\"user\"}":
                list9.append(float(tempdict[k]))
            if k == "node_cpu_seconds_total{cpu=\"1\",mode=\"user\"}":
                list10.append(float(tempdict[k]))     

        for k in maindict.keys():
            timesdict.setdefault(k, 1.0)
    else:
        tempdict = dict(zip(metricsname, valuelist))
        for k, v in tempdict.items():
            if k in maindict.keys():
                if k == "node_load1":
                    listnodeload1.append(float(tempdict[k]))
                if k == "node_load15":
                    list6.append(float(tempdict[k]))
                if k == "node_load5":
                    list7.append(float(tempdict[k]))
                if k == "node_memory_MemFree_bytes":
                    listmemory.append(float(tempdict[k]))
                if k == "node_disk_io_now{device=\"vda\"}":
                    listnodediskionow.append(float(tempdict[k]))
                if k == "node_memory_MemAvailable_bytes":
                    list8.append(float(tempdict[k]))
                if k == "node_network_transmit_bytes_total{device=\"ens3\"}":
                    list0.append(float(tempdict[k]))
                if k == "node_sockstat_TCP_alloc":
                    list1.append(float(tempdict[k]))
                if k == "node_cpu_seconds_total{cpu=\"1\",mode=\"idle\"}":
                    list2.append(float(tempdict[k]))
                if k == "node_cpu_seconds_total{cpu=\"0\",mode=\"system\"}":
                    list3.append(float(tempdict[k]))
                if k == "node_cpu_seconds_total{cpu=\"1\",mode=\"system\"}":
                    list4.append(float(tempdict[k]))
                if k == "node_cpu_seconds_total{cpu=\"0\",mode=\"idle\"}":
                    list5.append(float(tempdict[k]))
                if k == "node_cpu_seconds_total{cpu=\"0\",mode=\"user\"}":
                    list9.append(float(tempdict[k]))
                if k == "node_cpu_seconds_total{cpu=\"1\",mode=\"user\"}":
                    list10.append(float(tempdict[k]))                    
                maindict[k] = float(maindict[k])+float(v)
                timesdict[k] = float(timesdict[k]) + 1.0
            else:
                maindict[k] = float(v)
                timesdict.setdefault(k, 1.0)
    f.close()
    end = time.perf_counter()
    timewriter("merge" + " " + str(end-start))

def calcavg():
    start = time.perf_counter()
    global avgdict
    for k in maindict.keys():
        if k in timesdict.keys():
            maindict[k] = float(maindict[k])/float(timesdict[k])
    avgdict = maindict.copy()

    maindict.clear()
    end = time.perf_counter()
    timewriter("calcavg" + " " + str(end-start))

def calcstd(timestamp):
    print("calc std_dev")
    print(listmemory)
    print(listnodeload1)
    print(listnodediskionow)
    print(list8)
    print(list0)
    print(list1)
    print(list2)
    print(list3)
    print(list4)
    print(list5)
    print(list6)
    print(list7)
    print(list9)
    print(list10)

    std_dev = np.std(listmemory)
    means = np.mean(listmemory)
    skewness=skew(listmemory)
    errorpernode(timestamp,"node_memory_MemFree_bytes",std_dev,means,skewness,listmemory)
    listmemory.clear()

    std_dev = np.std(listnodeload1)
    means = np.mean(listnodeload1)
    skewness=skew(listnodeload1)
    errorpernode(timestamp,"node_load1",std_dev,means,skewness,listnodeload1)
    listnodeload1.clear()

    std_dev = np.std(listnodediskionow)
    means = np.mean(listnodediskionow)
    skewness=skew(listnodediskionow)
    errorpernode(timestamp,"node_disk_io_now{device=\"vda\"}",std_dev,means,skewness,listnodediskionow)
    listnodediskionow.clear()

    std_dev = np.std(list8)
    means = np.mean(list8)
    skewness=skew(list8)
    errorpernode(timestamp,"node_memory_MemAvailable_bytes",std_dev,means,skewness,list8)
    list8.clear()

    std_dev = np.std(list0)
    means = np.mean(list0)
    skewness=skew(list0)
    errorpernode(timestamp,"node_network_transmit_bytes_total{device=\"ens3\"}",std_dev,means,skewness,list0)
    list0.clear()

    std_dev = np.std(list1)
    means = np.mean(list1)
    skewness=skew(list1)
    errorpernode(timestamp,"node_sockstat_TCP_alloc",std_dev,means,skewness,list1)
    list1.clear()

    std_dev = np.std(list2)
    means = np.mean(list2)
    skewness=skew(list2)
    errorpernode(timestamp,"node_cpu_seconds_total{cpu=\"1\"mode=\"idle\"}",std_dev,means,skewness,list2)
    list2.clear()

    std_dev = np.std(list3)
    means = np.mean(list3)
    skewness=skew(list3)
    errorpernode(timestamp,"node_cpu_seconds_total{cpu=\"0\"mode=\"system\"}",std_dev,means,skewness,list3)
    list3.clear()

    std_dev = np.std(list4)
    means = np.mean(list4)
    skewness=skew(list4)
    errorpernode(timestamp,"node_cpu_seconds_total{cpu=\"1\"mode=\"system\"}",std_dev,means,skewness,list4)
    list4.clear()

    std_dev = np.std(list5)
    means = np.mean(list5)
    skewness=skew(list5)
    errorpernode(timestamp,"node_cpu_seconds_total{cpu=\"0\"mode=\"idle\"}",std_dev,means,skewness,list5)
    list5.clear()

    std_dev = np.std(list6)
    means = np.mean(list6)
    skewness=skew(list6)
    errorpernode(timestamp,"node_load15",std_dev,means,skewness,list6)
    list6.clear()

    std_dev = np.std(list7)
    means = np.mean(list7)
    skewness=skew(list7)
    errorpernode(timestamp,"node_load5",std_dev,means,skewness,list7)
    list7.clear()
    
    std_dev = np.std(list9)
    means = np.mean(list9)
    skewness=skew(list9)
    errorpernode(timestamp,"node_cpu_seconds_total{cpu=\"0\"mode=\"user\"}",std_dev,means,skewness,list9)
    list9.clear()
    
    std_dev = np.std(list10)
    means = np.mean(list10)
    skewness=skew(list10)
    errorpernode(timestamp,"node_cpu_seconds_total{cpu=\"1\"mode=\"user\"}",std_dev,means,skewness,list10)
    list10.clear()



def initmemory():
    start = time.perf_counter()
    maindict.clear()
    timesdict.clear()
    helplist.clear()
    checklist.clear()
    end = time.perf_counter()
    timewriter("cleardata" + " " + str(end-start))

if __name__ == "__main__":
    perparestart = time.perf_counter()

    prom_host=getControllerMasterIP()
    scrapeurl=gettargets(prom_host)
    lenoftarget=len(scrapeurl)
    clv=0
    f = open("error.csv", 'a')
    f.write("timestamp"+","+"metrics"+","+"std"+","+"average"+","+"std/average"+","+"skewness"+","+"list")
    f.write("\n")
    f.close
    BUFFER_SIZE = 8192
    HOST = '0.0.0.0'
    PORT = 54088
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(1)
    perpareend = time.perf_counter()
    timewriter("perpare"+ " "+ str(perpareend - perparestart))
    while 1:
        rounderror1=0
        rounderror2=0
        rounderror3=0
        print("Server start")
        print(scrapeurl)
        timestamp = time.time()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncgetmetrics(scrapeurl))

        for number in range(lenoftarget):
            name= "before" + str(number)
            merge(name)
        calcavg()
        calcstd(timestamp)
        initmemory()
        time.sleep(5)
        #time.sleep(60)