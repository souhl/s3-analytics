#########################################
## S3 Analytics Script
## v0.12
## by Sascha Uhl
#########################################


import os
import boto3
import boto3.session
import threading
import queue
import os
import datetime
from datetime import datetime
from botocore.exceptions import ClientError
import sys

class s3analytics(object):

    ## Connection Parameters
    global accessKey
    accessKey = "xxxxx"

    global secretKey
    secretKey = "yyyyyyyyyy"

    global endPoint
    endPoint = "http://s3-......."

    global bucketId
    bucketId = "BucketName"

    ## Number of Workers
    global threadCount
    threadCount = 128


    ## Internal
    global objects20k
    objects20k = 0
    global objects50k
    objects50k = 0
    global objects100k
    objects100k = 0
    global objects200k
    objects200k = 0
    global objects300k
    objects300k = 0
    global objects500k
    objects500k = 0
    global objects700k
    objects700k = 0
    global objects1m
    objects1m = 0
    global objects2m
    objects2m = 0
    global objects5m
    objects5m = 0
    global objects10m
    objects10m = 0
    global objects50m
    objects50m = 0
    global objects100m
    objects100m = 0
    global objects200m
    objects200m = 0
    global objects500m
    objects500m = 0
    global objectsLarge
    objectsLarge = 0

    global objectsTotalCount
    objectsTotalCount = 0

    global objectsTotalSize
    objectsTotalSize = 0

    global dateObjectLockLatest
    dateObjectLockLatest = 0

    global ObjectLockModeComplianceCount
    ObjectLockModeComplianceCount = 0

    global ObjectLockModeGovernanceCount
    ObjectLockModeGovernanceCount = 0

    global ObjectLockModeLegalHoldCount
    ObjectLockModeLegalHoldCount = 0

    global errorCount
    errorCount = 0

    global bucketVersioningEnabled
    bucketVersioningEnabled = False

    global bucketStoragePolicyID
    bucketStoragePolicyID = 0

    global bucketStorageRegion
    bucketStorageRegion = ""

    global bucketStorageServer
    bucketStorageServer = ""

    #global files_to_download
    #files_to_download = []


    def __init__(self):
        self.scanAndDump();

    def scanAndDump(self):

            session = boto3.Session()
            s3 = session.client("s3", aws_access_key_id=accessKey, aws_secret_access_key=secretKey, endpoint_url=endPoint)
            global lck 
            lck = threading.Lock()
            errorlogdate = datetime. now(). strftime("%Y_%m_%d-%I%M%S")
            errorlogfilename = f"s3analytics_{errorlogdate}.error"
            errorlog_f = open(errorlogfilename, "x")

            try:
                response = s3.get_bucket_versioning(Bucket=bucketId)
                
                if 'Status' in response and response['Status'] == 'Enabled':
                    global bucketVersioningEnabled
                    bucketVersioningEnabled = True
            except Exception as e:
                #print(e)
                sys.exit(e)

            try:
                response = s3.list_objects_v2(
                    Bucket=bucketId,
                    MaxKeys=2,
                )

                #teststr = response['ResponseMetadata']['HTTPHeaders']['x-gmt-policyid']
                #print(teststr)

                global bucketStoragePolicyID
                bucketStoragePolicyID = response['ResponseMetadata']['HTTPHeaders']['x-gmt-policyid']
                #print (str(bucketStoragePolicyID))

                global bucketStorageRegion
                bucketStorageRegion = response['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']
                #print (bucketStorageRegion)

                global bucketStorageServer
                bucketStorageServer = response['ResponseMetadata']['HTTPHeaders']['server']
                #print (bucketStorageServer)


  
            except Exception as e:
                #print(e)
                sys.exit(e)

            


            

            def writeError(text):
                global lck
                lck.acquire()
                errorlog_f.write(text+"\n")
                #errorlog_f.close()
                lck.release()

                global errorCount
                errorCount = errorCount + 1

            def printresults(): 
                def get_directory_size(directory):
                    """Returns the `directory` size in bytes."""
                    total = 0
                    try:
                        # print("[+] Getting the size of", directory)
                        for entry in os.scandir(directory):
                            if entry.is_file():
                                # if it's a file, use stat() function
                                total += entry.stat().st_size
                            elif entry.is_dir():
                                # if it's a directory, recursively call this function
                                total += get_directory_size(entry.path)
                    except NotADirectoryError:
                        # if `directory` isn't a directory, get the file size then
                        return os.path.getsize(directory)
                    except PermissionError:
                        # if for whatever reason we can't open the folder, return 0
                        return 0
                    return total

                def get_size_format(b, factor=1024, suffix="B"):
                    """
                    Scale bytes to its proper byte format
                    e.g:
                        1253656 => '1.20MB'
                        1253656678 => '1.17GB'
                    """
                    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
                        if b < factor:
                            return f"{b:.2f}{unit}{suffix}"
                        b /= factor
                    return f"{b:.2f}Y{suffix}"

                print('All work completed')

                date = datetime. now(). strftime("%Y_%m_%d-%I%M%S")

                filename = f"s3analytics_{date}.results"
                
                f = open(filename, "x")

                def write2screenandfile (txt):
                    print(txt)
                    f.write(txt+"\n")

                print("                                                            ", end='\r')
                print("                                                            ")
                print("                                                            ")
                #write2screenandfile("                                              ")
                write2screenandfile("OBJECT ANALYTICS                              ")
                write2screenandfile("##############################################")

                write2screenandfile("                                              ")
                #global endPoint
                write2screenandfile("S3 Vendor : " + bucketStorageServer)
                write2screenandfile("Endpoint  : " + endPoint)
                #global bucketId
                write2screenandfile("Bucket    : " + bucketId)
                write2screenandfile("Region    : " + bucketStorageRegion)
                if (bucketStorageServer == "CloudianS3"):
                    write2screenandfile("Cloudian Storage Policy : " + str(bucketStoragePolicyID))
                
                write2screenandfile(" ")
                write2screenandfile("Total Objects    : " + str(objectsTotalCount))
                write2screenandfile("Total Size (MB)  : " + get_size_format(objectsTotalSize)) #str(objectsTotalSize/1024/1024))
                write2screenandfile("Errors           : " + str(errorCount))
                write2screenandfile(" ")
                write2screenandfile("up to 20KB       : " + str(objects20k))
                write2screenandfile("20KB to 50KB     : " + str(objects50k))
                write2screenandfile("50KB to 100KB    : " + str(objects100k))
                write2screenandfile("100KB to 200KB   : " + str(objects200k))
                write2screenandfile("200KB to 300KB   : " + str(objects300k))
                write2screenandfile("300KB to 500KB   : " + str(objects500k))
                write2screenandfile("500KB to 700KB   : " + str(objects700k))
                write2screenandfile("700KB to 1MB     : " + str(objects1m))
                write2screenandfile("1MB to 2MB       : " + str(objects2m))
                write2screenandfile("2MB to 5MB       : " + str(objects5m))
                write2screenandfile("5MB to 10MB      : " + str(objects10m))
                write2screenandfile("10MB to 50MB     : " + str(objects50m))
                write2screenandfile("50MB to 100MB    : " + str(objects100m))
                write2screenandfile("200MB to 200MB   : " + str(objects200m))
                write2screenandfile("200MB to 500MB   : " + str(objects500m))
                write2screenandfile("Larger 500MB     : " + str(objectsLarge))
                write2screenandfile(" ")
                write2screenandfile(" ")
                write2screenandfile("OBJECT LOCK INFORMATION:")

                global ObjectLockModeGovernanceCount,ObjectLockModeComplianceCount,ObjectLockModeLegalHoldCount
                dt_object = datetime.fromtimestamp(dateObjectLockLatest)
                if (ObjectLockModeGovernanceCount+ObjectLockModeComplianceCount > 0):
                    write2screenandfile("Latest Object Lock set to       : " + str(dt_object))
                else:
                    write2screenandfile("Latest Object Lock set to       : NOT SET")
                write2screenandfile("Objects in Lock Mode Governance : " + str(ObjectLockModeGovernanceCount))
                write2screenandfile("Objects in Lock Mode Compliance : " + str(ObjectLockModeComplianceCount))
                write2screenandfile("Objects in Lock Mode Legal Hold : " + str(ObjectLockModeLegalHoldCount))

                global bucketVersioningEnabled
                bucketVersioningEnabled = True
                if bucketVersioningEnabled:
                    write2screenandfile("Bucket Versioning Enabled       : YES")
                else:
                    write2screenandfile("Bucket Versioning Enabled       : NO")

                print(" ")
                print("Output written to folder   : " + os.getcwd())
                print("Output written to filename : " + filename)
                f.close()


            def analyseObjectSizes(obSize):
                if obSize:
                    global objectsTotalCount
                    objectsTotalCount = objectsTotalCount + 1
                    global objectsTotalSize
                    objectsTotalSize = objectsTotalSize + obSize
                    global objectsLarge,objects20k,objects50k,objects100k,objects200k,objects500k,objects1m,objects700k,objects2m,objects5m,objects10m,objects50m,objects200m,objects100m,objects500m
                    if   obSize < 20000 : objects20k = objects20k + 1
                    elif obSize < 50000 : objects50k = objects50k + 1
                    elif obSize < 100000 : objects100k = objects100k + 1
                    elif obSize < 200000 : objects200k = objects200k + 1
                    elif obSize < 500000 : objects500k = objects500k + 1
                    elif obSize < 700000 : objects700k = objects700k + 1
                    elif obSize < 1024000 : objects1m = objects1m + 1
                    elif obSize < 2048000 : objects2m = objects2m + 1
                    elif obSize < 5120000 : objects5m = objects5m + 1
                    elif obSize < 10240000 : objects10m = objects10m + 1
                    elif obSize < 51200000 : objects50m = objects50m + 1
                    elif obSize < 102400000 : objects100m = objects100m + 1
                    elif obSize < 204800000 : objects200m = objects200m + 1
                    elif obSize < 512000000 : objects500m = objects500m + 1
                    else            : objectsLarge = objectsLarge + 1 

            def getobjects():
                try:
                    paginator = s3.get_paginator('list_objects_v2')
                    pages = paginator.paginate(Bucket=bucketId, Prefix='')
                except Exception as e:
                    error_code = e.response['Error']['Code']
                    sys.exit("Error during reading objects: ",error_code)

                for page in pages:
                    global objectsTotalCount
                    print("Reading Objects: " + str(objectsTotalCount), end='\r')
                    #print (page)
                    for obj in page['Contents']:
                        
                        analyseObjectSizes(obj['Size'])
                        q.put(obj['Key'])

            def compareDate(s3key):

                returnValue = True
                global bucketId
                fileobj = ""
                try:
                    fileobj = s3.get_object(Bucket=bucketId,Key=s3key) 
                except Exception as e:
                    global errorCount
                    errorCount = errorCount + 1
                    writeError(str(e) + "," + s3key)
                    print("Error: "+ str(e) + "," + s3key)
                    returnValue = False
                
                if 'ObjectLockRetainUntilDate' in fileobj:          
                    timestamp = datetime.timestamp(fileobj['ObjectLockRetainUntilDate'])
                
                    global dateObjectLockLatest
                    if timestamp > dateObjectLockLatest:
                        dateObjectLockLatest = timestamp
                
                global ObjectLockModeComplianceCount,ObjectLockModeGovernanceCount,ObjectLockModeLegalHoldCount

                if 'ObjectLockMode' in fileobj:
                    if fileobj['ObjectLockMode'] == "GOVERNANCE":
                        ObjectLockModeComplianceCount = ObjectLockModeComplianceCount + 1
                    elif fileobj['ObjectLockMode'] == "COMPLIANCE":
                        ObjectLockModeGovernanceCount = ObjectLockModeGovernanceCount + 1
                
                if 'ObjectLockLegalHoldStatus' in fileobj:
                    if fileobj['ObjectLockLegalHoldStatus'] == "ON":
                        ObjectLockModeLegalHoldCount = ObjectLockModeLegalHoldCount + 1

                if 'ObjectLockLegalHoldStatus' in fileobj:
                    if fileobj['ObjectLockLegalHoldStatus'] == "ON":
                        ObjectLockModeLegalHoldCount = ObjectLockModeLegalHoldCount + 1       

                return returnValue    
                
            q = queue.Queue()
            error_q = queue.Queue()

            def worker(count):
                while True:
                    print("Analysing Objects:                            ", end='\r')
                    print("Analysing Objects: " + str(q.qsize()), end='\r')
                    _task = q.get()
                    if compareDate(_task):
                        q.task_done()
                    else:
                        q.task_done()
                        q.put(_task)
                        
            #Load Objects
            getobjects()

            threads = list()

            global threadCount
            for index in range(threadCount):
                x = threading.Thread(target=worker, args=(index,))
                x.daemon = True
                threads.append(x)
                x.start()

            q.join()
            printresults()

            errorlog_f.close()

s3analyticsobj = s3analytics()
