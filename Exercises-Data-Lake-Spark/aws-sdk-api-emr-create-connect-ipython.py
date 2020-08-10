import pandas as pd
import boto3
import json
import configparser
from IPython import get_ipython
import psycopg2
import time



config = configparser.ConfigParser()
config.read_file(open('emr.cfg'))

#KEY                             = config.get('AWS','KEY')
#SECRET                          = config.get('AWS','SECRET')
EMR_CLUSTER_NAME                    = config.get('EMR','EMR_CLUSTER_NAME')
EMR_LOG_URI                         = config.get('EMR','EMR_LOG_URI')
EMR_RELEASE_LABEL                   = config.get('EMR','EMR_RELEASE_LABEL')
EMR_MASTER_INSTANCE_TYPE            = config.get('EMR','EMR_MASTER_INSTANCE_TYPE')
EMR_SLAVE_INSTANCE_TYPE             = config.get('EMR','EMR_SLAVE_INSTANCE_TYPE')
EMR_MASTER_INSTANCE_COUNT           = config.get('EMR','EMR_MASTER_INSTANCE_COUNT')
EMR_SLAVE_INSTANCE_COUNT            = config.get('EMR','EMR_SLAVE_INSTANCE_COUNT')
EMR_EC2_KEY_NAME                    = config.get('EMR','EMR_EC2_KEY_NAME')
EMR_JOB_FLOW_ROLE                   = config.get('EMR','EMR_JOB_FLOW_ROLE')
EMR_SERVICE_ROLE                    = config.get('EMR','EMR_SERVICE_ROLE')
EMR_MASTER_SG                       = config.get('EMR', 'EMR_MASTER_SG')
EMR_SLAVE_CORE_SG                   = config.get('EMR', 'EMR_SLAVE_CORE_SG')


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
#Run 'aws configure' to put the right credentials in ~/.aws/credentials
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

emr = session.client('emr',
                        region_name='us-west-1'
                  )


#Sampling an s3 bucket
sampleBucket =  s3.Bucket("kkd-udacity")
for obj in sampleBucket.objects.filter(Prefix=""):
    print(obj)


print("Printing existing IAM roles")
emrroleArn = iam.get_role(RoleName='EMR_DefaultRole')['Role']['Arn']
emrrole = iam.get_role(RoleName='EMR_DefaultRole')
emrec2roleArn = iam.get_role(RoleName='EMR_EC2_DefaultRole')['Role']['Arn']
emrec2role = iam.get_role(RoleName='EMR_EC2_DefaultRole')

print("\n emrroleArn: ")
print(emrroleArn)
print("\n emrrole: ")
print(emrrole)

print("\n emrec2roleArn: ")
print(emrec2roleArn)
print("\n emrec2role: ")
print(emrec2role)

print("\nrole/kkdtituiamrole1: ")
print(iam.get_role(RoleName="kkdtituiamrole1"))

print("\npolicy/kkd-udacity: ")
print(iam.get_policy(PolicyArn='arn:aws:iam::311694234399:policy/kkd-udacity'))

#creating an IAM role with SDK
#1.1 Create the role,
""" 
try:
    print("Creating a new IAM Role") 
    iam.create_role(
        Path='/',
        RoleName=EMR_IAM_ROLE_NAME,
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

iam.attach_role_policy(RoleName=EMR_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )


print("Printing new IAM roles")

print("\n EMR_IAM_ROLE_NAME)['Role']['Arn']: ")
print(iam.get_role(RoleName=EMR_IAM_ROLE_NAME)['Role']['Arn'])
print("\n EMR_IAM_ROLE_NAME): ")
print(iam.get_role(RoleName=EMR_IAM_ROLE_NAME))


# Deleting the SDK created IAM role -- you have to first detach a policy and then delete role
try:
    iam.detach_role_policy(RoleName=EMR_IAM_ROLE_NAME_SDK,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=EMR_IAM_ROLE_NAME_SDK)
except Exception as e:
    print(e)
"""


print("Now creating EMR cluster: ")
try:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.run_job_flow
    cluster_id = emr.run_job_flow(
        Name=EMR_CLUSTER_NAME,
        LogUri=EMR_LOG_URI,
        ReleaseLabel=EMR_RELEASE_LABEL,
        Applications=[
            {
            'Name': 'Spark' 
            #'Version': '2.4.5'   #cannot specify version for application 'Spark' when release label is used.
            },
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': EMR_MASTER_INSTANCE_TYPE,
                    'InstanceCount': int(EMR_MASTER_INSTANCE_COUNT),
                },
                {
                    'Name': "Slave",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': EMR_SLAVE_INSTANCE_TYPE,
                    'InstanceCount': int(EMR_SLAVE_INSTANCE_COUNT),
                }
            ],
            'Ec2KeyName': EMR_EC2_KEY_NAME,  
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'EmrManagedMasterSecurityGroup': EMR_MASTER_SG,
            'EmrManagedSlaveSecurityGroup': EMR_SLAVE_CORE_SG,
            #'Ec2SubnetId': 'subnet-id',
        },
        #Steps=[
        #    {
        #        'Name': 'file-copy-step',   
        #                'ActionOnFailure': 'CONTINUE',
        #                'HadoopJarStep': {
        #                    'Jar': 's3://Snapshot-jar-with-dependencies.jar',
        #                    'Args': ['test.xml', 'emr-test', 'kula-emr-test-2']
        #                }
        #    }
        #],
        VisibleToAllUsers=True,
        JobFlowRole=EMR_JOB_FLOW_ROLE,
        ServiceRole=EMR_SERVICE_ROLE,
        Tags=[
            {
                'Key': 'tag_name_1',
                'Value': 'tab_value_1',
            },
            {
                'Key': 'tag_name_2',
                'Value': 'tag_value_2',
            },
        ],
    )        
except Exception as e:
    print(e)

print("cluster_id = ", cluster_id)
cluster_description = emr.describe_cluster(ClusterId=cluster_id['JobFlowId'])
print("cluster_description = ", cluster_description)
cluster_status = cluster_description['Cluster']['Status']['State']
print("cluster_status = ", cluster_status)
cluster_state_change_reason = cluster_description['Cluster']['Status']['StateChangeReason']
print("cluster_state_change_reason = ", cluster_state_change_reason)


#waiting for cluster to get created 
while (('starting' in cluster_status.lower().strip()) | (not cluster_state_change_reason)): #checking if cluster_state_change_reason is empty dict
    print("Waiting 20 seconds as cluster is getting launched")
    time.sleep(20)
    cluster_description = emr.describe_cluster(ClusterId=cluster_id.get('JobFlowId'))
    cluster_status = cluster_description.get('Cluster').get('Status').get('State')
    print("cluster_status = ", cluster_status)
    cluster_state_change_reason = cluster_description.get('Cluster').get('Status').get('StateChangeReason')
    print("cluster_state_change_reason = ", cluster_state_change_reason)

print("Cluster creation complete\n")
cluster_description = emr.describe_cluster(ClusterId=cluster_id.get('JobFlowId'))
print("cluster_description = ", cluster_description)
cluster_status = cluster_description.get('Cluster').get('Status').get('State')
print("cluster_status = ", cluster_status)
cluster_state_change_reason = cluster_description.get('Cluster').get('Status').get('StateChangeReason').get('Message')
print("cluster_state_change_reason = ", cluster_state_change_reason)


ifdeletecluster = input("Do you want to deleter cluster ? ")
ifdeletecluster = ifdeletecluster.lower()

print("ifdeletecluster = ", ifdeletecluster)
if "yes" in ifdeletecluster:
    try:
        print("Trying to delete cluster ")
        emr.terminate_job_flows( 
            JobFlowIds=[
                cluster_id.get('JobFlowId')
            ]
        )
    except Exception as e:
        print(e)