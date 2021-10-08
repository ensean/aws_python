### s3国内海外同步检测说明

#### 方案说明

1. [此blog](https://aws.amazon.com/cn/blogs/china/lambda-overseas-china-s3-file/) 同步方案在极端情况下未获取到文件大小时会导致DDB无记录进而导致文件同步遗漏
2. 本方案通过定时触发，对比某一天内的所有文件来判断是否有遗漏文件，对于遗漏文件通过在源s3桶copy文件的方式触发重传。


#### 前置条件

1. 已参考[此blog](https://aws.amazon.com/cn/blogs/china/lambda-overseas-china-s3-file/)部署准实时同步方案
2. 环境准备
  * 海外s3桶ak、sk
  * sns topic，并创建对应的订阅

#### 本方案部署说明

1. 创建lambda函数，运行环境为Python3.6，代码为`s3_match_cn_global.py`
2. 根据提示配置lambda 环境变量
3. 为lamda执行角色附加以下权限
    * S3FullAccess
    * SNSFullAccess
4. 配置event rule定时触发Lambda