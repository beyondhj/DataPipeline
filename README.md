# DataPipeline
DataPipeline 是一款批流一体数据融合平台。无需任何代码，通过配置界面即可部署一条数据管道，提供功能：数据同步，数据质量分析、数据加工等功能。
本项目受DTStack/flinkx启发，感谢！

#配置样例
```json
{
    "creator": "JorJer",
    "version": "1",
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
            "type": "sql",
            "config": {
                "sql": "select *,case when id<10 then 'error' else 'ok' end as tag from t_order0"
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