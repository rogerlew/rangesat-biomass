# =============================================================================
#  USGS/EROS Inventory Service Example
#  Description: Download Landsat Collection 2 files
#  Usage: python download_sample.py -u username -p password -f filetype
#         optional argument f refers to filetype including 'bundle' or 'band'
# =============================================================================

import json
import requests
import sys
import time
import argparse
import re
import threading
import datetime

from os.path import join as _join
from os.path import exists as _exists
from os.path import split as _split

from glob import glob

path = ""  # Fill a valid download path
maxthreads = 5  # Threads count for downloads
sema = threading.Semaphore(value=maxthreads)
label = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")  # Customized label using date time
threads = []
# The entityIds/displayIds need to save to a text file such as scenes.txt.
# The header of text file should follow the format: datasetName|displayId or datasetName|entityId.
# sample file - scenes.txt
# landsat_ot_c2_l2|displayId
# LC08_L2SP_012025_20201215_20201219_02_T1
# LC08_L2SP_012027_20201215_20201219_02_T1
scenesFile = 'scenes.txt'


# Send http request
def sendRequest(url, data, apiKey=None, exitIfNoResponse=True):
    json_data = json.dumps(data)

    if apiKey == None:
        response = requests.post(url, json_data)
    else:
        headers = {'X-Auth-Token': apiKey}
        response = requests.post(url, json_data, headers=headers)

    try:
        httpStatusCode = response.status_code
        if response is None:
            print("No output from service")
            if exitIfNoResponse:
                sys.exit()
            else:
                return False
        output = json.loads(response.text)
        if output['errorCode'] is not None:
            print(output['errorCode'], "- ", output['errorMessage'])
            if exitIfNoResponse:
                sys.exit()
            else:
                return False
        if httpStatusCode == 404:
            print("404 Not Found")
            if exitIfNoResponse:
                sys.exit()
            else:
                return False
        elif httpStatusCode == 401:
            print("401 Unauthorized")
            if exitIfNoResponse:
                sys.exit()
            else:
                return False
        elif httpStatusCode == 400:
            print("Error Code", httpStatusCode)
            if exitIfNoResponse:
                sys.exit()
            else:
                return False
    except Exception as e:
        response.close()
        print(e)
        if exitIfNoResponse:
            sys.exit()
        else:
            return False
    response.close()

    return output['data']


def downloadFile(url):
    sema.acquire()
    try:
        response = requests.get(url, stream=True)
        disposition = response.headers['content-disposition']
        filename = re.findall("filename=(.+)", disposition)[0].strip("\"")
        print(f"Downloading {filename} ...\n")
        if path != "" and path[-1] != "/":
            filename = "/" + filename
        open(path + filename, 'wb').write(response.content)
        print(f"Downloaded {filename}\n")
        sema.release()
    except Exception as e:
        print(f"Failed to download from {url}. Will try to re-download.")
        sema.release()
        runDownload(threads, url)


def runDownload(threads, url):
    thread = threading.Thread(target=downloadFile, args=(url,))
    threads.append(thread)
    thread.start()


def parse_key(product_id):
    satellite = int(product_id[2:4])
    wrs_path = int(product_id[4:7])
    wrs_row = int(product_id[7:10])

    _date = product_id[10:18]
    year, month, day = int(_date[:4]), int(_date[4:6]), int(_date[6:])
    return satellite, wrs_path, wrs_row, year, month, day

def build_catalog(directory):
    fns = glob(_join(directory, '*.gz'))

    catalog = set()
    for fn in fns:
        product_id = _split(fn)[-1].split('-')[0]
        catalog.add(parse_key(product_id))

    return catalog


landsat_data_dir = "/geodata/torch-landsat"
catalog = build_catalog(landsat_data_dir)

#print(list(catalog)[:10])
#sys.exit()

if __name__ == '__main__':
    # User input
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--username', required=True, help='Username')
    parser.add_argument('-p', '--password', required=True, help='Password')
    parser.add_argument('-y', '--year', required=False, help='Year')
    parser.add_argument('-s', '--satellite', required=False, help='Satellite')
    parser.add_argument('-o', '--overwrite', required=False, help='Overwrite')
    parser.add_argument('-f', '--filetype', required=False, choices=['bundle', 'band'],
                        help='File types to download, "bundle" for bundle files and "band" for band files')

    args = parser.parse_args()

    username = args.username
    password = args.password
    filetype = args.filetype   

    print("\nRunning Scripts...\n")
    startTime = time.time()

    serviceUrl = "https://m2m.cr.usgs.gov/api/api/json/stable/"

    # Login
    payload = {'username': username, 'password': password}
    apiKey = sendRequest(serviceUrl + "login", payload)
    print("API Key: " + apiKey + "\n")

    # Read scenes
#    f = open(scenesFile, "r")
#    lines = f.readlines()
#    f.close()
#    header = lines[0].strip()
#    datasetName = header[:header.find("|")]
#    idField = header[header.find("|") + 1:]

#    print("Scenes details:")
#    print(f"Dataset name: {datasetName}")
#    print(f"Id field: {idField}\n")

    entityIds = []

#    lines.pop(0)
#    for line in lines:
#        entityIds.append(line.strip())

    # Search scenes
    # If you don't have a scenes text file that you can use scene-search to identify scenes you're interested in
    # https://m2m.cr.usgs.gov/api/docs/reference/#scene-search
    payload = {
                'datasetName' : 'gls_all',
                'maxResults' : 100, 
                'startingNumber' : 1,
                'sceneFilter' : 
                { 
                  'spatialFilter': 
                  {
                    'filterType': 'mbr',
                    'lowerLeft': {'latitude': 45.783, 'longitude': -117.252368 },
                    'upperRight': {'latitude': 45.789, 'longitude': -117.244277 }
                  },
                  'acquisitionFilter':
                  {
                    'end': '2021-05-17',
                    'start': '2000-01-01'
                  } 
                },
                'metadataType': 'summary',
                'sortDirection': 'ASC',
              }

    results = sendRequest(serviceUrl + "scene-search", payload, apiKey)
    from pprint import pprint
    pprint(results)
    for res in results['results']:
        entityId = res['entityId']
        if entityId.startswith('L'):
            key = parse_key(entityId)
            print(key, entityId)
            if key in catalog:
                pprint(res)
                entityIds.append(entityId)

    print(entityIds)
    sys.exit()

    # Add scenes to a list
    listId = f"temp_{datasetName}_list"  # customized list id
    payload = {
        "listId": listId,
        'idField': idField,
        "entityIds": entityIds,
        "datasetName": datasetName
    }

    print("Adding scenes to list...\n")
    count = sendRequest(serviceUrl + "scene-list-add", payload, apiKey)
    print("Added", count, "scenes\n")

    # Get download options
    payload = {
        "listId": listId,
        "datasetName": datasetName
    }

    print("Getting product download options...\n")
    products = sendRequest(serviceUrl + "download-options", payload, apiKey)
    print("Got product download options\n")

    # Select products
    downloads = []
    if filetype == 'bundle':
        # select bundle files
        for product in products:
            if product["bulkAvailable"]:
                downloads.append({"entityId": product["entityId"], "productId": product["id"]})
    elif filetype == 'band':
        # select band files
        for product in products:
            if product["secondaryDownloads"] is not None and len(product["secondaryDownloads"]) > 0:
                for secondaryDownload in product["secondaryDownloads"]:
                    if secondaryDownload["bulkAvailable"]:
                        downloads.append(
                            {"entityId": secondaryDownload["entityId"], "productId": secondaryDownload["id"]})
    else:
        # select all available files
        for product in products:
            if product["bulkAvailable"]:
                downloads.append({"entityId": product["entityId"], "productId": product["id"]})
                if product["secondaryDownloads"] is not None and len(product["secondaryDownloads"]) > 0:
                    for secondaryDownload in product["secondaryDownloads"]:
                        if secondaryDownload["bulkAvailable"]:
                            downloads.append(
                                {"entityId": secondaryDownload["entityId"], "productId": secondaryDownload["id"]})

    # Remove the list
    payload = {
        "listId": listId
    }
    sendRequest(serviceUrl + "scene-list-remove", payload, apiKey)

    # Send download-request
    payLoad = {
        "downloads": downloads,
        "label": label,
        'returnAvailable': True
    }

    print(f"Sending download request ...\n")
    results = sendRequest(serviceUrl + "download-request", payLoad, apiKey)
    print(f"Done sending download request\n")

    for result in results['availableDownloads']:
        print(f"Get download url: {result['url']}\n")
        runDownload(threads, result['url'])

    preparingDownloadCount = len(results['preparingDownloads'])
    preparingDownloadIds = []
    if preparingDownloadCount > 0:
        for result in results['preparingDownloads']:
            preparingDownloadIds.append(result['downloadId'])

        payload = {"label": label}
        # Retrieve download urls
        print("Retrieving download urls...\n")
        results = sendRequest(serviceUrl + "download-retrieve", payload, apiKey, False)
        if results != False:
            for result in results['available']:
                if result['downloadId'] in preparingDownloadIds:
                    preparingDownloadIds.remove(result['downloadId'])
                    print(f"Get download url: {result['url']}\n")
                    runDownload(threads, result['url'])

            for result in results['requested']:
                if result['downloadId'] in preparingDownloadIds:
                    preparingDownloadIds.remove(result['downloadId'])
                    print(f"Get download url: {result['url']}\n")
                    runDownload(threads, result['url'])

        # Don't get all download urls, retrieve again after 30 seconds
        while len(preparingDownloadIds) > 0:
            print(f"{len(preparingDownloadIds)} downloads are not available yet. Waiting for 30s to retrieve again\n")
            time.sleep(30)
            results = sendRequest(serviceUrl + "download-retrieve", payload, apiKey, False)
            if results != False:
                for result in results['available']:
                    if result['downloadId'] in preparingDownloadIds:
                        preparingDownloadIds.remove(result['downloadId'])
                        print(f"Get download url: {result['url']}\n")
                        runDownload(threads, result['url'])

    print("\nGot download urls for all downloads\n")
    # Logout
    endpoint = "logout"
    if sendRequest(serviceUrl + endpoint, None, apiKey) == None:
        print("Logged Out\n")
    else:
        print("Logout Failed\n")

    print("Downloading files... Please do not close the program\n")
    for thread in threads:
        thread.join()

    print("Complete Downloading")

    executionTime = round((time.time() - startTime), 2)
    print(f'Total time: {executionTime} seconds')
