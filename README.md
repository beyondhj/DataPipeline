# DataPipeline
DataPipeline 是一款批流一体数据融合平台。无需任何代码，通过配置界面即可部署一条数据管道，提供功能：数据同步，数据传输、数据分发等功能。

  <div align=center>
     <img src=docs/images/data-pipeline-common-api.png width=600 />
   </div>

计划分为下面几个模块：

**1. datapipeline-core**

Spark任务。

**2. datapipeline-designer**

图形化配置。

**3. datapipeline-scheduler**

任务调度。

# 配置样例
```json
{
  "jobType": "batch",
  "version": "1.0",
  "setting": {
    "appName": "DatabaseTest",
    "spark.sql.shuffle.partitions": 200
  },
  "nodes": [
    {
      "id": "1",
      "name": "输入",
      "type": "reader",
      "config": {
        "format": "jdbc",
        "driver": "com.mysql.jdbc.Driver",
        "url": "jdbc:mysql://localhost/test?serverTimezone=UTC",
        "dbtable": "t_order0",
        "user": "root",
        "password": "root"
      }
    },
    {
      "id": "2",
      "name": "过滤",
      "type": "filter",
      "config": {
        "filter": "id > 0 and price > 1"
      }
    },
    {
      "id": "3",
      "name": "输出",
      "type": "writer",
      "format": "jdbc",
      "config": {
        "driver": "com.mysql.jdbc.Driver",
        "url": "jdbc:mysql://localhost/test?serverTimezone=UTC",
        "dbtable": "t_order1",
        "user": "root",
        "password": "root"
      }
    }
  ],
  "edges": [
    {
      "source": "1",
      "target": "2"
    },
    {
      "source": "2",
      "target": "3"
    }
  ]
}
```

本项目受DTStack/flinkx启发，感谢！
