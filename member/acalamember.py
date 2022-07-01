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


timeout_seconds = 30
maindict = {}
timesdict = {}
helplist = []
checklist = []
lastmaindict = {}
avgdict = {}
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
        f = open("error", 'a')
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
            #if item["labels"]["instance"] != "10.158.4.2:9100":
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
        tasks = [asyncio.create_task(fetch(link, session, links.index(link))) for link in links]  # 建立任務清單
        await asyncio.gather(*tasks)

def merge(path):
    start = time.perf_counter()
    f = open(path, 'r')
    global maindict
    global timesdict
    global checklist
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

def error(path,rate):
    start = time.perf_counter()
    f = open(path, 'r')
    global maindict
    global avgdict
    global timesdict
    global checklist
    counterformetrics = 1
    valuelist = []
    metricsname = []
    tempdict = {}
    all=0
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
    
    maindict = dict(zip(metricsname, valuelist))
    i=0
    same=0
    for k in maindict.keys():
        if k in avgdict.keys():
            #print(avgdict[k],maindict[k])
            #if (float(maindict[k])*1.05) >= float(avgdict[k]) and (float(maindict[k])*0.95) <= float(avgdict[k]):
                #print(float(maindict[k]),float(avgdict[k]))
            if rate==0:
                if float(maindict[k])==float(avgdict[k]):
                    same+=1
            elif rate==1:
                if (float(maindict[k])*1.05) >= float(avgdict[k]) and (float(maindict[k])*0.95) <= float(avgdict[k]):
                    same+=1
            elif rate==2:
                if (float(maindict[k])*1.1) >= float(avgdict[k]) and (float(maindict[k])*0.90) <= float(avgdict[k]):
                    same+=1
            #print(maindict[k])
            # if maindict[k]=="0":
            #     j=0.00001
            # else:
            #     j=maindict[k]
            # errorrate = abs(float(maindict[k])-float(avgdict[k]))/float(j)
            # errorwriter(str(k) + ": " +str(errorrate))
            # all=all+errorrate
        i+=1
    print(i)
    print(same)
    print(same/i)
    #avgerror=float(all)/float(i)
    #errorpernode(str(avgerror))
    
    f.close()
    return same,i,same/i


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
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncgetmetrics(scrapeurl))
        timestamp = time.time()
        for number in range(lenoftarget):
            name= "before" + str(number)
            merge(name)
        calcavg()
        for number in range(lenoftarget):
            name= "before" + str(number)
            same,i,sameavg=error(name,0)
            errorpernode(str(timeforsave)+","+str(name)+","+"rate=0,"+str(same)+","+str(i)+","+str(sameavg))
            rounderror1=rounderror1+sameavg

            same,i,sameavg=error(name,1)
            errorpernode(str(timeforsave)+","+str(name)+","+"rate=0.05,"+str(same)+","+str(i)+","+str(sameavg))
            rounderror2=rounderror2+sameavg

            same,i,sameavg=error(name,2)
            errorpernode(str(timeforsave)+","+str(name)+","+"rate=0.1,"+str(same)+","+str(i)+","+str(sameavg))
            rounderror3=rounderror3+sameavg

        errorpernode(str(timeforsave)+","+str(rounderror1)+","+"rate=0,"+str(lenoftarget)+","+str(rounderror1/lenoftarget))
        errorpernode(str(timeforsave)+","+str(rounderror2)+","+"rate=0.05,"+str(lenoftarget)+","+str(rounderror2/lenoftarget))
        errorpernode(str(timeforsave)+","+str(rounderror3)+","+"rate=0.1,"+str(lenoftarget)+","+str(rounderror3/lenoftarget))
        initmemory()
        time.sleep(5)