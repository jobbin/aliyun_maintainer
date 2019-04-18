import os
import oss2
import zipfile
import json
import logging
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.acs_exception.exceptions import ClientException
from aliyunsdkcore.acs_exception.exceptions import ServerException
from aliyunsdkecs.request.v20140526 import AddTagsRequest
from aliyunsdkrds.request.v20140815.AddTagsToResourceRequest import AddTagsToResourceRequest


def handler(event, context):
  logger = logging.getLogger()
  logger.info(event)

  ACCESS_KEY_ID = os.environ['ACCESS_KEY_ID']
  ACCESS_KEY_SECRET = os.environ['ACCESS_KEY_SECRET']

  #OSS関連初期設定
  Event = json.loads(event.decode('utf-8').replace("'", '"'))
  OssRegion = Event["events"][0]["region"]
  BuketName = Event["events"][0]["oss"]["bucket"]["name"]
  ObjectName = Event["events"][0]["oss"]["object"]["key"]
  OssEndPoint = "oss-" + OssRegion +".aliyuncs.com"

  # ECS, RDS関連初期設定
  InstanceIdSet = []
  UserName = ""
  Region = ""
  TagName = os.environ['TAG_NAME']

  # OSS
  auth = oss2.Auth(ACCESS_KEY_ID, ACCESS_KEY_SECRET)
  bucket = oss2.Bucket(auth, OssEndPoint, BuketName)

  tmpdir = '/tmp/download/'
  os.system("rm -rf /tmp/*")
  os.mkdir(tmpdir)

  #対象ActionTrailログをOSSからダウンロード
  bucket.get_object_to_file(ObjectName , tmpdir + 'trail_log.gz')
  os.system("gunzip /tmp/download/trail_log.gz")

  with open('/tmp/download/trail_log') as data:
    OssNotification = json.load(data)

  for actionTrailLog in OssNotification:
    logger.info("*"*20)
    logger.info("eventName : " + actionTrailLog["eventName"])
    logger.info("acsRegion : " + actionTrailLog["acsRegion"])
    logger.info("*"*20)

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
          # TODO
          # 複数の場合の処理、type(InstanceIdSet) == str !!!!!!!!!
          InstanceIdSet = actionTrailLog["responseElements"]["DBInstanceId"]


      #logger.info(InstanceIdSet)
      UserName = actionTrailLog["userIdentity"]["userName"]
      AcsRegion = actionTrailLog["acsRegion"]

    else:
      logger.info("Isn't target event !")
      return 0

  #TARGET_EVENTS instanceにOwnerタグを追加
  client = AcsClient(ACCESS_KEY_ID, ACCESS_KEY_SECRET, AcsRegion)


  for instance in InstanceIdSet :
    
    if actionTrailLog["eventName"] == "CreateDBInstance":
      request = AddTagsToResourceRequest()
      request.set_accept_format('json')
      logger.info(instance)
      # request.set_Tags(str)
      request.set_Tags({TagName: UserName})
      request.set_DBInstanceId(instance)
    else:  
      Tags = [{"Key": TagName,"Value": UserName}]
      request = AddTagsRequest.AddTagsRequest()
      request.set_ResourceType("instance")
      request.set_Tags(Tags)
      request.set_ResourceId(instance)

    client.do_action_with_exception(request)
    print(str(response, encoding='utf-8'))
  return 0