import json
import boto3
import datetime
import os


global_ak = os.environ['global_ak']
global_sk = os.environ['global_sk']
cn_s3_bucket = os.environ['cn_s3_bucket']
cn_s3_prefix = os.environ['cn_s3_prefix']
global_s3_bucket = os.environ['global_s3_bucket']
global_s3_prefix = os.environ['global_s3_prefix']
alarm_sns_arn = os.environ['alarm_sns_arn']
target_region_name = os.environ['target_region_name']
temp_pagination = {'PageSize': 1000}


def lambda_handler(event, context):
    # TODO implement
    s3_cn = boto3.client("s3")
    s3_global = boto3.client("s3", region_name=target_region_name, 
                        aws_access_key_id=global_ak, 
                        aws_secret_access_key=global_sk)
    date_prefix = get_date_prefix()
    cn_complete_prefix = cn_s3_prefix + date_prefix
    global_complete_prefix = global_s3_prefix + date_prefix
    cn_s3_list = get_s3_obj_list(s3_cn, cn_s3_bucket, cn_complete_prefix)
    global_s3_list = get_s3_obj_list(s3_global, global_s3_bucket, global_complete_prefix)
    not_match = match_s3_list(global_s3_list, cn_s3_list)
    if len(not_match) == 0:
        print("Great Result")
        return {
                'statusCode': 200,
                'body': json.dumps('Great Result')
            }
    not_upload = re_upload(not_match, cn_complete_prefix, s3_cn)
    alarm(not_match, not_upload, cn_complete_prefix)
    print("find unsynchronized files")
    return {
                'statusCode': 200,
                'body': json.dumps('Find Unsynchronized Files')
            }


# 检测获取两小时前的日期分区
def get_date_prefix():
    current = datetime.datetime.now() + datetime.timedelta(hours=-2)
    year_str = str(current.year)
    month = current.month
    day = current.day
    month_str = str(month)
    if month < 10:
        month_str = "0" + month_str
    day_str = str(day)
    if day < 10:
        day_str = "0" + day_str
    date_prefix = year_str + "/" + month_str + "/" + day_str + "/"
    return date_prefix


def match_s3_list(global_s3_list, cn_s3_list):
    return list(set(cn_s3_list).difference(set(global_s3_list)))


# reupload to source bucket
def re_upload(not_match, prefix, s3_client):
    not_upload = []
    for item in not_match:
        key_path = prefix + item
        response = s3_client.copy_object(Bucket=cn_s3_bucket, Key=key_path,
                                         Metadata={'reupload': str(datetime.datetime.now())},
                                         StorageClass='STANDARD',
                                         CopySource={'Bucket': cn_s3_bucket, 'Key': key_path})
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            not_upload.append(item)
    return not_upload


# prefix： use cn prefix
def alarm(not_match, not_upload, prefix):
    sns = boto3.client("sns")
    not_match_str = "此次检测，检测到以下文件没有同步：\n"
    for item in not_match:
        not_match_str += "\t\t%s%s\n" % (prefix, item)
    not_upload_str = "此次重传，检测到以下文件没有成功：\n"
    for item in not_upload:
        not_upload_str += "\t\t%s%s\n" % (prefix, item)
    if len(not_upload) == 0:
        not_upload_str = "此次已经检测结果全部重传完成"
    msg = "桶名称：%s \n\n%s\n\n%s" % (cn_s3_bucket, not_match_str, not_upload_str)
    response = sns.publish(
        TopicArn=alarm_sns_arn,
        Message=msg,
        Subject='S3 国内海外同步警报',
    )


# get s3 object list
def get_s3_obj_list(s3_client, bucket_name, prefix):
    
    # 仅检测1小时前上传的文件，避免将正在复制中的文件判定为遗漏文件
    time_pivot = datetime.datetime.now() + datetime.timedelta(hours=-1)

    s3_obj_list = []
    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=bucket_name,
        PaginationConfig=temp_pagination,
        Prefix=prefix
    )
    s3_iter = response_iterator.__iter__()
    response = s3_iter.__next__()
    while True:
        if 'Contents' not in response.keys():
            break
        contents = response['Contents']
        for content in contents:
            if content['LastModified'].timestamp() < time_pivot.timestamp():
                add_key = content['Key'].split(prefix)[1]
                s3_obj_list.append(add_key)
        if 'NextToken' not in response.keys():
            break
        response = s3_iter.__next__()
    return s3_obj_list
