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
        jsondata = json.dumps(data, indent=2)
        f.write(f'{jsondata}')
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


    }
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
            line = f"vehicles,{keys[:-1]} {fields[:-1]}"
            print(line)
            pb['load'].append(line)

    def getTrips(pburl):
        allTrips = {}
        feed = parseDict(pburl)
        for value in feed['entity']:
            print(value)


    realtime_list = [
        'https://www.metrostlouis.org/RealTimeData/StlRealTimeVehicles.pb',
        'https://www.metrostlouis.org/RealTimeData/StlRealTimeTrips.pb'
    ]

    getVehicles(gtfsrt['vehicles'])
    print(gtfsrt['vehicles']['load'])

    # for item in realtime_list:
    #     # if looking at vehicles
    #     if item == 'https://www.metrostlouis.org/RealTimeData/StlRealTimeVehicles.pb':
    #         print('writing vehicles...')
    #         vehicles = getVehicles(item)
    #         saveTempData(vehicles, r'leaflet\vehicles.json')
    #         pass
        # if looking at the trips file
        # elif item == 'https://www.metrostlouis.org/RealTimeData/StlRealTimeTrips.pb':
        #     print('writing trips...')
        #     print(item)
        #     trips = getTrips(item)
        #     saveTempData(trips, r'leaflet\trips.json')
        #     pass
        # else:
        #     print(item)
        #     print('error')
        #     return

# getGTFS()

# for file in gtfs:
#     loadGTFS(file)

# for file in gtfs:
#     print(file['path'])
#     print(file['load'])

getRealTime(gtfsrt)
#
#
# while 1==1:
#   getRealTime()
#   time.sleep(15)
