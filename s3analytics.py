#########################################
## S3 Analytics Script
## v0.13
## by Sascha Uhl
#########################################


import os
import boto3
import boto3.session
from tqdm import tqdm
import threading
import queue
import os
import datetime
from datetime import datetime
import sys
from time import sleep

class s3analytics(object):

    ## Connection Parameters

    ## S3 Access Key 
    ACCESSKEY = "00d74f022207e1efbc34"

    ## S3 Secret Key 
    SECRETKEY = "ph5vWLgxfMSeG9JIKVpvtGatxirTuWpw798pMP3f"

    ## S3 Endpoint 
    ENDPOINT = "http://s3-munich.souhl.lab"

    ## Bucket name to analyse
    BUCKETID = "test"

    ## Number of Workers
    THREADCOUNT = 128

    def __init__(self):

        self.bucketStorageServer = ""

        self.objects20k = 0
        self.objects50k = 0
        self.objects100k = 0
        self.objects200k = 0
        self.objects300k = 0
        self.objects500k = 0
        self.objects700k = 0
        self.objects1m = 0
        self.objects2m = 0
        self.objects5m = 0
        self.objects10m = 0
        self.objects50m = 0
        self.objects100m = 0
        self.objects200m = 0
        self.objects500m = 0
        self.objectsLarge = 0

        self.folderDepths = {}

        self.TotalCountObjects = 0
        self.objectsTotalSize = 0
        self.dateObjectLockLatest = 0

        self.ObjectLockModeComplianceCount = 0
        self.ObjectLockModeGovernanceCount = 0
        self.ObjectLockModeLegalHoldCount = 0

        self.errorCount = 0

        self.bucketVersioningEnabled = False
        self.bucketStoragePolicyID = 0
        self.bucketStorageRegion = ""

        self.q = queue.Queue()
        self.error_q = queue.Queue()

        self.session = boto3.Session()
        self.s3 = self.session.client("s3", aws_access_key_id=s3analytics.ACCESSKEY, aws_secret_access_key=s3analytics.SECRETKEY, endpoint_url=s3analytics.ENDPOINT)

        errorlogdate = datetime. now(). strftime("%Y_%m_%d-%I%M%S")
        self.errorlogfilename = f"s3analytics_{errorlogdate}.error"
        self.errorlog_f = open(self.errorlogfilename, "x")

        date = datetime. now(). strftime("%Y_%m_%d-%I%M%S")
        self.filename = f"s3analytics_{date}.results"
        self._resultFile = open(self.filename, "x")
        
        self.main()

    def main(self):

            self.lck = threading.Lock()  

            def getFolderDepth(self,object):
                
                sentence = object
                depth = sentence.count('/')

                depthCount = 0
                if depth in self.folderDepths: 
                    depthCount = self.folderDepths[depth]
                    depthCount = depthCount + 1
                    #print(".")
                w1 = {depth:depthCount}
                self.folderDepths.update(w1)
                #print (self.folderDepths)

            def getBucketInformation(self):
                try:
                    response = self.s3.get_bucket_versioning(Bucket=s3analytics.BUCKETID)
                
                    if 'Status' in response and response['Status'] == 'Enabled':
                        self.bucketVersioningEnabled = True

                except Exception as e:
                    sys.exit(e)

                try:
                    response = self.s3.list_objects_v2(
                        Bucket=s3analytics.BUCKETID,
                        MaxKeys=2,
                    )

                    self.bucketStoragePolicyID = response['ResponseMetadata']['HTTPHeaders']['x-gmt-policyid']
                    self.bucketStorageRegion = response['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']
                    self.bucketStorageServer = response['ResponseMetadata']['HTTPHeaders']['server']

                except Exception as e:
                    sys.exit(e)
        
            def writeError(self,text):
                self.lck.acquire()
                self.errorlog_f.write(text+"\n")
                self.lck.release()

                self.errorCount = self.errorCount + 1

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

            def write2screenandfile (txt,file):
                    print(txt)
                    file.write(txt+"\n")

            def printresults(self): 
            
                print('All work completed')
                print("                                                            ", end='\r')
                print("                                                            ")
                print("                                                            ")

                write2screenandfile("OBJECT ANALYTICS                              ",self._resultFile)
                write2screenandfile("##############################################",self._resultFile)

                write2screenandfile("                                              ",self._resultFile)
                write2screenandfile("CONNECTION PARAMETERS                              ",self._resultFile)
                write2screenandfile("##############################################",self._resultFile)
                write2screenandfile("S3 Vendor : " + self.bucketStorageServer,self._resultFile)
                write2screenandfile("Endpoint  : " + s3analytics.ENDPOINT,self._resultFile)
                write2screenandfile("Bucket    : " + s3analytics.BUCKETID,self._resultFile)
                write2screenandfile("Region    : " + self.bucketStorageRegion,self._resultFile)
                if (self.bucketStorageServer == "CloudianS3"):
                    write2screenandfile("Cloudian Storage Policy : " + str(self.bucketStoragePolicyID),self._resultFile)
                
                write2screenandfile(" ",self._resultFile)
                write2screenandfile("OBJECTS SUMMARY                              ",self._resultFile)
                write2screenandfile("##############################################",self._resultFile)
                write2screenandfile("Total Objects    : " + str(self.TotalCountObjects),self._resultFile)
                write2screenandfile("Total Size (MB)  : " + get_size_format(self.objectsTotalSize),self._resultFile)
                write2screenandfile("Errors           : " + str(self.errorCount),self._resultFile)
                write2screenandfile(" ",self._resultFile)
                write2screenandfile("OBJECT SIZES with amount of objects                             ",self._resultFile)
                write2screenandfile("##############################################",self._resultFile)
                write2screenandfile("up to 20KB       : " + str(self.objects20k),self._resultFile)
                write2screenandfile("20KB to 50KB     : " + str(self.objects50k),self._resultFile)
                write2screenandfile("50KB to 100KB    : " + str(self.objects100k),self._resultFile)
                write2screenandfile("100KB to 200KB   : " + str(self.objects200k),self._resultFile)
                write2screenandfile("200KB to 300KB   : " + str(self.objects300k),self._resultFile)
                write2screenandfile("300KB to 500KB   : " + str(self.objects500k),self._resultFile)
                write2screenandfile("500KB to 700KB   : " + str(self.objects700k),self._resultFile)
                write2screenandfile("700KB to 1MB     : " + str(self.objects1m),self._resultFile)
                write2screenandfile("1MB to 2MB       : " + str(self.objects2m),self._resultFile)
                write2screenandfile("2MB to 5MB       : " + str(self.objects5m),self._resultFile)
                write2screenandfile("5MB to 10MB      : " + str(self.objects10m),self._resultFile)
                write2screenandfile("10MB to 50MB     : " + str(self.objects50m),self._resultFile)
                write2screenandfile("50MB to 100MB    : " + str(self.objects100m),self._resultFile)
                write2screenandfile("200MB to 200MB   : " + str(self.objects200m),self._resultFile)
                write2screenandfile("200MB to 500MB   : " + str(self.objects500m),self._resultFile)
                write2screenandfile("Larger 500MB     : " + str(self.objectsLarge),self._resultFile)
                write2screenandfile(" ",self._resultFile)
                write2screenandfile(" ",self._resultFile)
                write2screenandfile("BUCKET FOLDER DEPTHS with amount of objects",self._resultFile)
                write2screenandfile("##############################################",self._resultFile)
                for key in self.folderDepths:
                    write2screenandfile("Depth " + str(key) + ": " + str(self.folderDepths[key]),self._resultFile)

            def printresults2(self):
                write2screenandfile(" ",self._resultFile)
                write2screenandfile(" ",self._resultFile)
                write2screenandfile("OBJECT LOCK INFORMATION:",self._resultFile)
                write2screenandfile("##############################################",self._resultFile)

                dt_object = datetime.fromtimestamp(self.dateObjectLockLatest)
                if (self.ObjectLockModeGovernanceCount+self.ObjectLockModeComplianceCount > 0):
                    write2screenandfile("Latest Object Lock set to       : " + str(dt_object),self._resultFile)
                else:
                    write2screenandfile("Latest Object Lock set to       : NOT SET",self._resultFile)
                write2screenandfile("Objects in Lock Mode Governance : " + str(self.ObjectLockModeGovernanceCount),self._resultFile)
                write2screenandfile("Objects in Lock Mode Compliance : " + str(self.ObjectLockModeComplianceCount),self._resultFile)
                write2screenandfile("Objects in Lock Mode Legal Hold : " + str(self.ObjectLockModeLegalHoldCount),self._resultFile)

                bucketVersioningEnabled = True
                if bucketVersioningEnabled:
                    write2screenandfile("Bucket Versioning Enabled       : YES",self._resultFile)
                else:
                    write2screenandfile("Bucket Versioning Enabled       : NO",self._resultFile)

                print(" ")
                write2screenandfile("OUTPUT:",self._resultFile)
                write2screenandfile("##############################################",self._resultFile)
                print("Output written to folder   : " + os.getcwd())
                print("Output written to filename : " + self.filename)
                self._resultFile.close()

            def analyseObjectSizes(self,obSize):
                if obSize:

                    #self.TotalCountObjects = self.TotalCountObjects + 1
                    self.objectsTotalSize = self.objectsTotalSize + obSize

                    if   obSize < 20000 : self.objects20k = self.objects20k + 1
                    elif obSize < 50000 : self.objects50k = self.objects50k + 1
                    elif obSize < 100000 : self.objects100k = self.objects100k + 1
                    elif obSize < 200000 : self.objects200k = self.objects200k + 1
                    elif obSize < 500000 : self.objects500k = self.objects500k + 1
                    elif obSize < 700000 : self.objects700k = self.objects700k + 1
                    elif obSize < 1024000 : self.objects1m = self.objects1m + 1
                    elif obSize < 2048000 : self.objects2m = self.objects2m + 1
                    elif obSize < 5120000 : self.objects5m = self.objects5m + 1
                    elif obSize < 10240000 : self.objects10m = self.objects10m + 1
                    elif obSize < 51200000 : self.objects50m = self.objects50m + 1
                    elif obSize < 102400000 : self.objects100m = self.objects100m + 1
                    elif obSize < 204800000 : self.objects200m = self.objects200m + 1
                    elif obSize < 512000000 : self.objects500m = self.objects500m + 1
                    else            : self.objectsLarge = self.objectsLarge + 1 

            def getobjects(self):
                
                try:
                    paginator = self.s3.get_paginator('list_objects_v2')
                    pages = paginator.paginate(Bucket=s3analytics.BUCKETID, Prefix='')
                except Exception as e:
                    error_code = e.response['Error']['Code']
                    sys.exit("Error during reading objects: ",error_code)

                print("Receiving list of objects...")

                t = tqdm(total=1000)
                with t as pbar:
                    for page in pages:
                        t.reset()
                        for obj in page['Contents']:
                            analyseObjectSizes(self,obj['Size'])
                            getFolderDepth(self,obj['Key']) 
                            self.q.put(obj['Key'])
                            t.update(1)
                            t.refresh()
                            
                        
                        

            def compareDate(self,s3key,objectDir):

                returnValue = True

                fileobj = ""
                try:
                    fileobj = self.s3.get_object(Bucket=s3analytics.BUCKETID,Key=s3key) 
                except Exception as e:
                    self.errorCount = self.errorCount + 1
                    writeError(self,str(e) + "," + s3key)
                    print("Error: "+ str(e) + "," + s3key)
                    returnValue = False
                
                timestamp = ""
                if 'ObjectLockRetainUntilDate' in fileobj:          
                    timestamp = datetime.timestamp(fileobj['ObjectLockRetainUntilDate'])
                    if timestamp > self.dateObjectLockLatest:
                        self.dateObjectLockLatest = timestamp

                objectSize = 0
                if 'ContentLength' in fileobj:
                    objectSize = fileobj['ContentLength']

                lastModifiedDate = ""
                if 'LastModified' in fileobj:          
                    lastModifiedDate = datetime.timestamp(fileobj['LastModified'])
                    
                objectLockMode = ""
                if 'ObjectLockMode' in fileobj:
                    if fileobj['ObjectLockMode'] == "GOVERNANCE":
                        self.ObjectLockModeComplianceCount = self.ObjectLockModeComplianceCount + 1
                        objectLockMode = "GOVERNANCE"

                    elif fileobj['ObjectLockMode'] == "COMPLIANCE":
                        self.ObjectLockModeGovernanceCount = self.ObjectLockModeGovernanceCount + 1
                        objectLockMode = "COMPLIANCE"
                
                objectLockLegalHold = "OFF"
                if 'ObjectLockLegalHoldStatus' in fileobj:
                    if fileobj['ObjectLockLegalHoldStatus'] == "ON":
                        self.ObjectLockModeLegalHoldCount = self.ObjectLockModeLegalHoldCount + 1
                        objectLockLegalHold = "ON"

                if (returnValue == True):

                    self.lck.acquire()
                    objectDir.setdefault(s3key, [])
                    objectDir[s3key].append(timestamp)
                    objectDir[s3key].append(int(objectSize))
                    objectDir[s3key].append(lastModifiedDate)
                    objectDir[s3key].append(objectLockMode)
                    objectDir[s3key].append(objectLockLegalHold)

                    self.lck.release()


                return returnValue    

            def worker(self,count,bar,objectDir):
                #objectDir = {"s3key":[]}

                while True:
                    #print("Analysing Objects:                            ", end='\r')
                    #print("Analysing Objects: " + str(self.q.qsize()), end='\r')

                    _task = self.q.get()
                    if compareDate(self,_task,objectDir):
                        self.q.task_done()
                        bar.update(1)
                        #print("Thread: "+str(count) +" / Objects: "+str(len(objectDir)))
                    else:
                        self.q.task_done()
                        self.q.put(_task)


            def startThreads(self):

                #Load Objects
                

                print("")
                print("Analysing Object Headers...")
                with tqdm(total=self.q.qsize()) as pbar:
                        
                        threads = list()
                        for index in range(s3analytics.THREADCOUNT):
                            objectDir = {}
                            objectGroup.append(objectDir)
                            x = threading.Thread(target=worker, args=(self,index,pbar,objectGroup[index]))
                            x.daemon = True
                            threads.append(x)
                            x.start()

                        self.q.join()

            print()
            getBucketInformation(self)
            objectGroup = []
            
            getobjects(self)

            self.TotalCountObjects = self.q.qsize()
            printresults(self)
            startThreads(self)

            printresults2(self)


            self.errorlog_f.close()

s3analyticsobj = s3analytics()
