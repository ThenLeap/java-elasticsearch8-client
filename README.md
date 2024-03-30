# 0. 写在前面

项目配置

JAVA GraalVM 17

ElasticSearch 8.3.3

Spring-Boot 2.7.2

Junit5 5.8.2

# 1. 开发目的

Elastic官方已从7.15.0开始，放弃支持旧版本中的Java REST Client（High Level Rest Client (HLRC)）。

替换为官方推荐使用的Java API Client 8.x,此项目即使用官方推荐的Java API Client 8.x重新编写ES的java客户端功能。

# 2. API类型

| 类型 | 用途     | 
|:---------|:-------| 
| [NodeApi](https://gitee.com/darkranger/java-elasticsearch8-client/blob/master/src/main/java/com/wujunshen/elasticsearch/NodeApi.java)       | 节点相关操作 | 
| [IndexApi](https://gitee.com/darkranger/java-elasticsearch8-client/blob/master/src/main/java/com/wujunshen/elasticsearch/IndexApi.java)       | 索引相关操作 | 
| [DocumentApi](https://gitee.com/darkranger/java-elasticsearch8-client/blob/master/src/main/java/com/wujunshen/elasticsearch/DocumentApi.java) | 文档相关操作 | 
| [QueryApi ](https://gitee.com/darkranger/java-elasticsearch8-client/blob/master/src/main/java/com/wujunshen/elasticsearch/QueryApi.java)    | 搜索相关操作 | 

# 3. 单元测试

见test下的 [elasticsearch](https://gitee.com/darkranger/java-elasticsearch8-client/tree/master/src/test/java/com/wujunshen/elasticsearch) 目录里的java测试类
