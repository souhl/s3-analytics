# s3-analytics
Script to retrieve object details via S3

Requires:
- Python 3
- Boto3

Sample Output:
```
OBJECT ANALYTICS                              
##############################################
                                              
CONNECTION PARAMETERS                              
##############################################
S3 Vendor : CloudianS3
Endpoint  : http://s3-munich.souhl.lab
Bucket    : test
Region    : munich
Cloudian Storage Policy : 267686b864b15acde424771d6476d0b3
 
OBJECTS SUMMARY                              
##############################################
Total Objects    : 7281
Total Size (MB)  : 83.27MB
Errors           : 0
 
OBJECT SIZES with amount of objects                             
##############################################
up to 20KB       : 5451
20KB to 50KB     : 330
50KB to 100KB    : 139
100KB to 200KB   : 117
200KB to 300KB   : 0
300KB to 500KB   : 79
500KB to 700KB   : 10
700KB to 1MB     : 8
1MB to 2MB       : 1
2MB to 5MB       : 1
5MB to 10MB      : 0
10MB to 50MB     : 0
50MB to 100MB    : 0
200MB to 200MB   : 0
200MB to 500MB   : 0
Larger 500MB     : 0
 
 
BUCKET FOLDER DEPTHS with amount of objects
##############################################
Depth 1: 19
Depth 2: 64
Depth 3: 858
Depth 4: 102
Depth 5: 178
Depth 6: 1307
Depth 7: 553
Depth 8: 940
Depth 9: 1852
Depth 10: 1196
Depth 11: 177
Depth 12: 23
 
OBJECT LOCK INFORMATION:
##############################################
Latest Object Lock set to       : NOT SET
Objects in Lock Mode Governance : 0
Objects in Lock Mode Compliance : 0
Objects in Lock Mode Legal Hold : 0
Bucket Versioning Enabled       : YES
 
OUTPUT:
##############################################
Output written to folder   : /Users/souhl/Desktop/s3-dev/boto3
Output written to filename : s3analytics_2021_07_07-120311.results
```
