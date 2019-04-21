import os
import oss2
import zipfile
import json
import logging
import time
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.acs_exception.exceptions import ClientException
from aliyunsdkcore.acs_exception.exceptions import ServerException
from aliyunsdkecs.request.v20140526 import AddTagsRequest
from aliyunsdkrds.request.v20140815.AddTagsToResourceRequest import AddTagsToResourceRequest
from aliyunsdkrds.request.v20140815.DescribeDBInstancesRequest import DescribeDBInstancesRequest


def handler(event, context):
  logger = logging.getLogger()
  logger.info(event)

  ACCESS_KEY_ID = os.environ['ACCESS_KEY_ID']
  ACCESS_KEY_SECRET = os.environ['ACCESS_KEY_SECRET']

  # RDS 確認用の初期設定, max time = 240s
  LIMITTED_COUNT  = 10
  SLEEP_TIME      = 24

  # ECS, RDS関連初期設定
  InstanceIdSet = []
  UserName      = ""
  Region        = ""
  TagName       = os.environ['TAG_NAME']

  #　OSS関連初期設定
  Event       = json.loads(event.decode('utf-8').replace("'", '"'))
  OssRegion   = Event["events"][0]["region"]
  BuketName   = Event["events"][0]["oss"]["bucket"]["name"]
  ObjectName  = Event["events"][0]["oss"]["object"]["key"]
  OssEndPoint = "oss-" + OssRegion +".aliyuncs.com"



  # OSS, client生成
  auth = oss2.Auth(ACCESS_KEY_ID, ACCESS_KEY_SECRET)
  bucket = oss2.Bucket(auth, OssEndPoint, BuketName)

  # ディレクトリの初期化
  tmpdir = '/tmp/download/'
  os.system("rm -rf /tmp/*")
  os.mkdir(tmpdir)

  #　対象ActionTrailログをOSSからダウンロード
  bucket.get_object_to_file(ObjectName , tmpdir + 'trail_log.gz')
  os.system("gunzip /tmp/download/trail_log.gz")

  with open('/tmp/download/trail_log') as data:
    OssNotification = json.load(data)

  for actionTrailLog in OssNotification:
    logger.info("eventName : " + actionTrailLog["eventName"])
    logger.info("acsRegion : " + actionTrailLog["acsRegion"])

    TARGET_EVENTS = ["RunInstances", "CreateInstance", "CreateDBInstance"]
    if actionTrailLog["eventName"] in TARGET_EVENTS :

      if actionTrailLog["eventName"] == "RunInstances" :
        InstanceIdSet = actionTrailLog["responseElements"]["InstanceIdSets"]["InstanceIdSet"]

      if actionTrailLog["eventName"] == "CreateInstance" :
        InstanceIdSet.append(actionTrailLog["responseElements"]["InstanceId"])

      if actionTrailLog["eventName"] == "CreateDBInstance":
        
        if actionTrailLog["requestParameters"]["Quantity"] == 1:
          # Quantity = 1
          InstanceIdSet.append(actionTrailLog["responseElements"]["DBInstanceId"])
        else: 
          # Quantity > 1
          # [注意] type(actionTrailLog["responseElements"]["DBInstanceId"]) == str !
          tmp_data = actionTrailLog["responseElements"]["DBInstanceId"]
          InstanceIdSet = tmp_data.replace('[', '').replace(']', '').replace('"', '').split(",")

      UserName = actionTrailLog["userIdentity"]["userName"]
      AcsRegion = actionTrailLog["acsRegion"]

    else:
      logger.info("Isn't target event !")
      return 0

  #TARGET_EVENTS instanceにOwnerタグを追加
  client = AcsClient(ACCESS_KEY_ID, ACCESS_KEY_SECRET, AcsRegion)

  for instance in InstanceIdSet :
    
    if actionTrailLog["eventName"] == "CreateDBInstance":
    # CreateDBInstance

      # 新規作成DBの情報を取得できたかの確認
      count = 0
      while count < LIMITTED_COUNT:
        request = DescribeDBInstancesRequest()
        response = json.loads(client.do_action_with_exception(request))
      
        # 既存のDBの情報を取得
        existed_db_list = []
        for data in response["Items"]["DBInstance"]:
          existed_db_list.append(data["DBInstanceId"])
        
        # 確認
        if instance not in existed_db_list:
          time.sleep(SLEEP_TIME)
          count += 1
        else:
          break

      # タグ付け
      request = AddTagsToResourceRequest()
      request.set_accept_format('json')
      request.set_Tags({TagName: UserName})
      request.set_DBInstanceId(instance)
      
    else:
    # RunInstances,CreateInstance  
      Tags = [{"Key": TagName,"Value": UserName}]
      request = AddTagsRequest.AddTagsRequest()
      request.set_ResourceType("instance")
      request.set_Tags(Tags)
      request.set_ResourceId(instance)

    client.do_action_with_exception(request)
  return 0