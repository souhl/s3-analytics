import boto3

class s3analytics(object):

    global accessKey
    accessKey = ""

    global secretKey
    secretKey = ""

    global endPoint
    endPoint = ""

    global bucketId
    bucketId = ""

    global targetBucket

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

    def __init__(self):
        self.scanAndDump();

    def scanAndDump(self):

            s3 = boto3.client('s3', aws_access_key_id=accessKey, aws_secret_access_key=secretKey, endpoint_url=endPoint)

            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucketId, Prefix='')

            for page in pages:
                global objectsTotalCount
                print("Object Count: " + str(objectsTotalCount), end='\r')
                for obj in page['Contents']:
                    obSize = obj['Size']

                    if obSize:
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


        
            print(" ", end='\r')
            print("OBJECT COUNTS and SIZES")
            print(" ")
            print("Total Objects    : " + str(objectsTotalCount))
            print("Total Size (MB)   : " + str(objectsTotalSize/1024/1024))
            print(" ")
            print("up to 20KB       : " + str(objects20k))
            print("20KB to 50KB     : " + str(objects50k))
            print("50KB to 100KB    : " + str(objects100k))
            print("100KB to 200KB   : " + str(objects200k))
            print("200KB to 300KB   : " + str(objects300k))
            print("300KB to 500KB   : " + str(objects500k))
            print("500KBup to 700KB : " + str(objects700k))
            print("700KB to 1MB     : " + str(objects1m))
            print("1MB to 2MB     : " + str(objects2m))
            print("2MB to 5MB     : " + str(objects5m))
            print("5MB to 10MB    : " + str(objects10m))
            print("10MB to 50MB    : " + str(objects50m))
            print("50MB to 100MB   : " + str(objects100m))
            print("200MB to 200MB   : " + str(objects200m))
            print("200MB to 500MB   : " + str(objects500m))
            print("Larger 500MB  : " + str(objectsLarge))
            print(" ")

s3analyticsobj = s3analytics()
