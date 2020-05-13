from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import requests
import os
import zipfile
from urllib.request import urlopen
import json
import csv

dir = os.path.join(os.getcwd(), 'gtfs')

# don't need this due to
def saveTempData(data, filename):
    with open(filename, "w") as f:
        f.write(f'{data}')
        f.close()

    print(' ')
    print("************************************")
    print(f'{filename} created!')
    print("************************************")
    print(' ')

gtfs = [
    {
        'path': os.path.join(dir, 'agency.txt'),
        'load': [],
    },
    {
       'path': os.path.join(dir, 'calendar.txt'),
        'load': [],
    },
    {
        'path': os.path.join(dir, 'calendar_dates.txt'),
        'load': [],
    },
    {
        'path': os.path.join(dir, 'routes.txt'),
        'load': [],
    },
    {
        'path': os.path.join(dir, 'shapes.txt'),
        'load': [],
    },
    {
        'path': os.path.join(dir, 'stop_times.txt'),
        'load': [],
    },
    {
        'path': os.path.join(dir, 'stops.txt'),
        'load': [],
    },
    {
        'path': os.path.join(dir, 'transfers.txt'),
        'load': [],
    },
    {
        'path': os.path.join(dir, 'trips.txt'),
        'load': [],
    }
]

gtfsrt = {
    'vehicles': {
        'url': 'https://www.metrostlouis.org/RealTimeData/StlRealTimeVehicles.pb',
        'load': [],
    },
    'trips': {
        'url': 'https://www.metrostlouis.org/RealTimeData/StlRealTimeTrips.pb',
        'load': [],
    },
}


def getGTFS():
    for file in gtfs:
        if os.path.exists(file):
            os.remove(file)
    print("**********************************************")
    print("STARTING GTFS FETCH...")
    print("**********************************************")
    url = 'https://metrostlouis.org/Transit/google_transit.zip'

    print('FETCHING GTFS...')

    zipresp = urlopen(url) # Create a new file on the hard drive
    tempzip = open("google_transit.zip", "wb") # Write the contents of the downloaded file into the new file
    tempzip.write(zipresp.read()) # Close the newly-created file
    tempzip.close() # Re-open the newly-created file with ZipFile()

    zf = zipfile.ZipFile("google_transit.zip") # Extract its contents into <extraction_path> *note that extractall will automatically create the path
    zf.extractall(dir) # close the ZipFile instance
    zf.close()
    os.remove(fr"{dir}/google_transit.zip")

    print(f'FETCHED GTFS => {dir}')
    print("**********************************************")
    print("ENDING GTFS FETCH...")
    print("**********************************************")
    print(' ')

def loadGTFS(file):
    with open(file['path'], newline='') as csvfile:
        lineString = f"{file['path'].split(dir)[1].split('.txt')[0][1:]}"  # get the file name without '.csv' and assign to measurement
        reader = csv.DictReader(csvfile)
        for line in reader:
            keys = ""
            fields = ""
            for field in line:
                if '_id' in field or field == "parent_station":
                    if 'ï»¿' in field:
                        f = field.split('ï»¿')[1]
                        keys += f'{f}:{line[field]},'
                    else:
                        keys += f'{field}:{line[field]},'
                else:
                    fields = f'{field}:{line[field]},'
            file['load'].append(f'{lineString},{keys[:-1]} {fields[:-1]} ')


def getRealTime(gtfsrt):
    def parseDict(pbu):
        # TAKES THE DATA FROM U (THE PB URL) AND TURNS IT INTO A DICTIONARY
        feed = gtfs_realtime_pb2.FeedMessage()
        url = pbu
        response = requests.get(url)
        feed.ParseFromString(response.content)
        feed2 = MessageToDict(feed)
        return feed2

    def getVehicles(pb):
        feed = parseDict(pb['url'])
        for value in feed['entity']:
            keys = ''
            fields = ''
            timestamp = value['vehicle']['timestamp']
            for data in value['vehicle']:
                if type(value['vehicle'][data]) == dict:
                    for d in value['vehicle'][data]:
                        if 'id' in d.lower():
                            keys += f'{d}:{value["vehicle"][data][d]},'
                        else:
                            if type(value["vehicle"][data][d]) == str:
                                fields += f'{d}:"{value["vehicle"][data][d]}",'
                            else:
                                fields += f'{d}:{value["vehicle"][data][d]},'
            line = f"vehicles,{keys[:-1]} {fields[:-1]} {timestamp}"
            print(line)
            pb['load'].append(line)
        return pb['load']


    def getTrips(pb):
        feed = parseDict(pb['url'])
        for value in feed['entity']:
            print(value)
            # {'id': '0', 'tripUpdate': {'trip': {'tripId': '2705906', 'startTime': '06:25:00', 'startDate': '20200513', 'routeId': '16370'}, 'stopTimeUpdate': [{'departure': {'time': '1589369100'}, 'stopId': '14403'}, {'departure': {'time': '1589369580'}, 'stopId': '4804'}, {'departure': {'time': '1589369640'}, 'stopId': '4806'}, {'departure': {'time': '1589369700'}, 'stopId': '4786'}, {'departure': {'time': '1589369760'}, 'stopId': '4810'}, {'departure': {'time': '1589369760'}, 'stopId': '4811'}, {'departure': {'time': '1589369820'}, 'stopId': '4814'}, {'departure': {'time': '1589369940'}, 'stopId': '4818'}, {'departure': {'time': '1589370060'}, 'stopId': '4822'}, {'departure': {'time': '1589370300'}, 'stopId': '13967'}, {'departure': {'time': '1589370540'}, 'stopId': '13526'}, {'departure': {'time': '1589370600'}, 'stopId': '15223'}, {'departure': {'time': '1589370660'}, 'stopId': '15623'}, {'departure': {'time': '1589370720'}, 'stopId': '15224'}, {'departure': {'time': '1589370780'}, 'stopId': '4826'}, {'departure': {'time': '1589370840'}, 'stopId': '4827'}, {'departure': {'time': '1589370840'}, 'stopId': '4828'}, {'departure': {'time': '1589370900'}, 'stopId': '15860'}, {'departure': {'time': '1589371020'}, 'stopId': '4829'}, {'departure': {'time': '1589371080'}, 'stopId': '14863'}, {'departure': {'time': '1589371080'}, 'stopId': '14862'}, {'departure': {'time': '1589371140'}, 'stopId': '14861'}, {'departure': {'time': '1589371200'}, 'stopId': '15732'}, {'departure': {'time': '1589371200'}, 'stopId': '4832'}, {'departure': {'time': '1589371440'}, 'stopId': '4834'}, {'departure': {'time': '1589371620'}, 'stopId': '15767'}, {'departure': {'time': '1589371620'}, 'stopId': '16065'}, {'departure': {'time': '1589371680'}, 'stopId': '15225'}, {'departure': {'time': '1589371860'}, 'stopId': '15012'}, {'departure': {'time': '1589371860'}, 'stopId': '15758'}, {'departure': {'time': '1589371920'}, 'stopId': '15733'}, {'departure': {'time': '1589371980'}, 'stopId': '15985'}, {'departure': {'time': '1589372040'}, 'stopId': '13528'}, {'departure': {'time': '1589372040'}, 'stopId': '587'}, {'departure': {'time': '1589372100'}, 'stopId': '13954'}, {'departure': {'time': '1589372160'}, 'stopId': '14828'}, {'departure': {'time': '1589372220'}, 'stopId': '14801'}, {'departure': {'time': '1589372340'}, 'stopId': '14133'}], 'vehicle': {'id': '4536', 'label': '65 - Outer Forty'}, 'timestamp': '1589371918'}}
            keys = ''
            fields = ''
            timestamp = value['tripUpdate']['timestamp']
            for data in value['tripUpdate']:
                # print(value['tripUpdate'][data])
                # print(type(value['tripUpdate'][data]))
                if type(value["tripUpdate"][data]) == dict:
                    for d in value["tripUpdate"][data]:
                        if 'id' in d.lower():
                            keys += f'{d}:{value["tripUpdate"][data][d]},'
                        else:
                            if type(value["tripUpdate"][data][d]) == str:
                                fields += f'{d}:"{value["tripUpdate"][data][d]}",'
                            else:
                                fields += f'{d}:{value["tripUpdate"][data][d]},'
            line = f"vehicles,{keys[:-1]} {fields[:-1]} {timestamp}"
            print(line)
            pb['load'].append(line)
        return pb['load']


    saveTempData(getVehicles(gtfsrt['vehicles']), 'vehicles.txt')
    saveTempData(getTrips(gtfsrt['trips']), 'trips.txt')
    print(gtfsrt['trips']['load'])

getRealTime(gtfsrt)


# while 1==1:
#   getRealTime()
#   time.sleep(15)
