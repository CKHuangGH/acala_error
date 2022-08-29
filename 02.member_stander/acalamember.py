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

def errorpernode(text):
    try:
        f = open("error.csv", 'a')
    except:
        print("Open exectime failed")
    try:
        f.write(text)
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
            if k == "node_memory_MemFree_bytes":
                listmemory.append(float(maindict[k]))
            if k == "node_disk_io_now{device=\"vda\"}":
                listnodediskionow.append(float(maindict[k]))
            if k == "node_network_receive_bytes_total{device=\"ens3\"}":
                listnode_network_receive_bytes_total.append(float(maindict[k]))
            if k == "node_network_transmit_bytes_total{device=\"ens3\"}":
                list0.append(float(maindict[k]))
            if k == "node_sockstat_TCP_alloc":
                list1.append(float(maindict[k]))
            if k == "node_sockstat_TCP_inuse":
                list2.append(float(maindict[k]))
            if k == "node_disk_read_bytes_total{device=\"vda\"}":
                list3.append(float(maindict[k]))
            if k == "node_disk_written_bytes_total{device=\"vda\"}":
                list4.append(float(maindict[k]))
            if k == "node_cpu_seconds_total{cpu=\"0\",mode=\"idle\"}":
                list5.append(float(maindict[k]))
            if k == "node_load15":
                list6.append(float(maindict[k]))
            if k == "node_load5":
                list7.append(float(maindict[k]))
        for k in maindict.keys():
            timesdict.setdefault(k, 1.0)
    else:
        tempdict = dict(zip(metricsname, valuelist))
        for k, v in tempdict.items():
            if k in maindict.keys():
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

    std_dev = np.std(listmemory)
    errorpernode(str(timestamp)+","+"node_memory_MemFree_bytes" +","+ str(std_dev))
    listmemory.clear()

    std_dev = np.std(listnodeload1)
    errorpernode(str(timestamp)+","+"node_load1" +","+ str(std_dev))
    listnodeload1.clear()

    std_dev = np.std(listnodediskionow)
    errorpernode(str(timestamp)+","+"node_disk_io_now{device=\"vda\"}" +","+ str(std_dev))
    listnodediskionow.clear()

    std_dev = np.std(listnode_network_receive_bytes_total)
    errorpernode(str(timestamp)+","+"node_network_receive_bytes_total{device=\"ens3\"}" +","+ str(std_dev))
    listnode_network_receive_bytes_total.clear()

    std_dev = np.std(list0)
    errorpernode(str(timestamp)+","+"node_network_transmit_bytes_total{device=\"ens3\"}" +","+ str(std_dev))
    list0.clear()

    std_dev = np.std(list1)
    errorpernode(str(timestamp)+","+"node_sockstat_TCP_alloc" +","+ str(std_dev))
    list1.clear()

    std_dev = np.std(list2)
    errorpernode(str(timestamp)+","+"node_sockstat_TCP_inuse" +","+ str(std_dev))
    list2.clear()

    std_dev = np.std(list3)
    errorpernode(str(timestamp)+","+"node_disk_read_bytes_total{device=\"vda\"}" +","+ str(std_dev))
    list3.clear()

    std_dev = np.std(list4)
    errorpernode(str(timestamp)+","+"node_disk_written_bytes_total{device=\"vda\"}" +","+ str(std_dev))
    list4.clear()

    std_dev = np.std(list5)
    errorpernode(str(timestamp)+","+"node_cpu_seconds_total{cpu=\"0\",mode=\"idle\"}" +","+ str(std_dev))
    list5.clear()

    std_dev = np.std(list6)
    errorpernode(str(timestamp)+","+"node_load15" +","+ str(std_dev))
    list6.clear()

    std_dev = np.std(list7)
    errorpernode(str(timestamp)+","+"node_load5" +","+ str(std_dev))
    list7.clear()


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
        timestamp = time.time()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncgetmetrics(scrapeurl))

        for number in range(lenoftarget):
            name= "before" + str(number)
            merge(name)
        calcavg()
        calcstd(timestamp)
        #errorpernode(str(timestamp)+","+str(rounderror1)+","+"rate=0,"+str(lenoftarget)+","+str(rounderror1/lenoftarget))
        #errorpernode(str(timestamp)+","+str(rounderror2)+","+"rate=0.05,"+str(lenoftarget)+","+str(rounderror2/lenoftarget))
        #errorpernode(str(timestamp)+","+str(rounderror3)+","+"rate=0.1,"+str(lenoftarget)+","+str(rounderror3/lenoftarget))
        initmemory()
        time.sleep(5)
        #time.sleep(60)