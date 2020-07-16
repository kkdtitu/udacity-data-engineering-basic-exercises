import pandas as pd
import boto3
import json
import configparser
from IPython import get_ipython
import psycopg2
import time



config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

#KEY                             = config.get('AWS','KEY')
#SECRET                          = config.get('AWS','SECRET')
DWH_IAM_ROLE_NAME_CONSOLE       = config.get('DWH', 'DWH_IAM_ROLE_NAME_CONSOLE')
DWH_IAM_ROLE_NAME               = config.get('DWH', 'DWH_IAM_ROLE_NAME')

DWH_CLUSTER_TYPE                = config.get('DWH','DWH_CLUSTER_TYPE')
DWH_NODE_TYPE                   = config.get('DWH','DWH_NODE_TYPE')
DWH_NUM_NODES                   = config.get('DWH','DWH_NUM_NODES')
DWH_CLUSTER_IDENTIFIER          = config.get('DWH','DWH_CLUSTER_IDENTIFIER')
DWH_DB                          = config.get('DWH','DWH_DB')
DWH_DB_USER                     = config.get('DWH','DWH_DB_USER')
DWH_DB_PASSWORD                 = config.get('DWH','DWH_DB_PASSWORD')
DWH_PORT                        = config.get('DWH','DWH_PORT')

"""
ec2 = boto3.resource('ec2',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

s3 = boto3.resource('s3',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

iam = boto3.client('iam',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-east-1'
                  )

redshift = boto3.client('redshift',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
"""

#boto3.session method will automatically pickup the credentials from [default] profile in ~/.aws/credentials file
#Run 'aws configure' put the right credentials in ~/.aws/credentials
session = boto3.Session(profile_name='default')

ec2 = session.resource('ec2',
                       region_name="us-east-1"
                    )

s3 = session.resource('s3',
                       region_name="us-east-1"
                   )

iam = session.client('iam',
                        region_name="us-east-1"
                  )


redshift = session.client('redshift',
                       region_name="us-east-1"
                       )

#Sampling an s3 bucket
sampleBucket =  s3.Bucket("kkd-udacity")
for obj in sampleBucket.objects.filter(Prefix=""):
    print(obj)


print("Printing existing IAM roles")
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME_CONSOLE)['Role']['Arn']
role = iam.get_role(RoleName=DWH_IAM_ROLE_NAME_CONSOLE)

print("\n DWH_IAM_ROLE_NAME_CONSOLE)['Role']['Arn']: ")
print(roleArn)
print("\n DWH_IAM_ROLE_NAME_CONSOLE): ")
print(role)

print("\nrole/kkdtituiamrole1: ")
print(iam.get_role(RoleName="kkdtituiamrole1"))

print("\npolicy/kkd-udacity: ")
print(iam.get_policy(PolicyArn='arn:aws:iam::311694234399:policy/kkd-udacity'))

#creating an IAM role with SDK
#1.1 Create the role, 
try:
    print("Creating a new IAM Role") 
    iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )    
except Exception as e:
    print(e)
    
    
print("Attaching Policy to newly created role")

iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )


print("Printing new IAM roles")

print("\n DWH_IAM_ROLE_NAME)['Role']['Arn']: ")
print(iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn'])
print("\n DWH_IAM_ROLE_NAME): ")
print(iam.get_role(RoleName=DWH_IAM_ROLE_NAME))

"""
# Deleting the SDK created IAM role -- you have to first detach a policy and then delete role
try:
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME_SDK,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME_SDK)
except Exception as e:
    print(e)
"""

print("Now creating Redshift cluster: ")
try:
    #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html
    redshift.create_cluster(        
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']],

        #VpcId='vpc-00f2c9d86d9e0d205' (cannot set VPC with boto3, will use default VPC)

        VpcSecurityGroupIds=['sg-f616a4b9', 'sg-0c8d5fd7004d635af'], 
        #ClusterSecurityGroups=['sg-f616a4b9', 'sg-0c8d5fd7004d635af'], 
        #can use either ClusterSecurityGroups or VpcSecurityGroupIds but NOT both at same time

        ClusterSubnetGroupName='udacity-cluster-subnet-group',
        #if ClusterSubnetGroupName is not specified, cluster will not be created in a VPC
        #https://docs.aws.amazon.com/redshift/latest/mgmt/getting-started-cluster-in-vpc.html

        PubliclyAccessible=True,
        Encrypted=False,
        EnhancedVpcRouting=False
    )
except Exception as e:
    print(e)


myClusterProps0 = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
print("Redshift cluster properties0:\n", myClusterProps0)
print("Redshift cluster properties:\n", myClusterProps)

#waiting for cluster to get created 
while not (('available' in myClusterProps.get('ClusterStatus')) & ('Available' in myClusterProps['ClusterAvailabilityStatus'])):
    print("ClusterStatus Status: ", myClusterProps.get('ClusterStatus'), "\t ClusterAvailabilityStatus: ", myClusterProps['ClusterAvailabilityStatus'])
    print("Waiting 20 seconds as cluster is getting launched")
    time.sleep(20)
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]


# The myClusterProps JSON/dict object does not have the 'Endpoint' key until cluster is available 
print("Cluster creation complete\n")
print("Redshift cluster endpoint:\n", myClusterProps.get('Endpoint', 'Null'))
print("Redshift cluster endpoint :\n", myClusterProps['Endpoint'])

DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

"""
# This section does NOT set up the security group properly (Redshift, TCP, 5439, 0.0.0.0/0) and should NOT be used
try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)
"""

# Connecting to the database
ip = get_ipython()
print(ip)
get_ipython().run_line_magic('load_ext', 'sql')

conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)

print("Connecting to DWH using %s: ", conn_string)
get_ipython().run_line_magic('sql', '$conn_string')
#once a connection is opened with the DWH_DB_USER and since there is only one user for now , the cluster status becomes 'unavailable'


ifdeletecluster = input("Do you want to deleter cluster ? ")
ifdeletecluster = ifdeletecluster.lower()

if "yes" or "y" in ifdeletecluster:
    try:
        redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(e)