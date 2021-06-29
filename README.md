# s3-analytics
Script to retrieve object details via S3

Requires:
- Python 3
- Boto3

Sample Output:
```
OBJECT ANALYTICS                              
##############################################
                                              
S3 Vendor : CloudianS3
Endpoint  : http://s3-munich.souhl.lab
Bucket    : test
Region    : munich
Cloudian Storage Policy : 267686b864b15acde424771d6476d0b3
 
Total Objects    : 6136
Total Size (MB)  : 83.27MB
Errors           : 0
 
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
 
 
OBJECT LOCK INFORMATION:
Latest Object Lock set to       : NOT SET
Objects in Lock Mode Governance : 0
Objects in Lock Mode Compliance : 0
Objects in Lock Mode Legal Hold : 0
Bucket Versioning Enabled       : YES
 
Output written to folder   : /Users/souhl/Desktop/s3-dev/boto3
Output written to filename : s3analytics_2021_06_29-112302.results
```
