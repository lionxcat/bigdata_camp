# 0725——使用Java API操作HBase

## 文件说明
- src/main/java/com/lionxcat/App.java/App.getConnection()：获取HBase连接。
- src/main/java/com/lionxcat/App.java/App.createTable()：创建表。
- src/main/java/com/lionxcat/App.java/App.resetTable()：删除已经创建的表和命名空间，方便重新开始。
- src/main/java/com/lionxcat/App.java/App.putColumn()：插入行。
- src/main/java/com/lionxcat/App.java/App.printTable()：使用scan来batch扫描表数据。
- src/main/java/com/lionxcat/App.java/App.getId()：使用get来获取某一学生name的student_id。
- 运行结果数据在HBase集群表baoxiao:student中。
- printTable打印表结果如下：
```
Rowkey : BaoXiao    Familiy:Quilifier : class    Value : 5    Timestamp : 1627833364326
Rowkey : BaoXiao    Familiy:Quilifier : student_id    Value : 20210607020479    Timestamp : 1627833364326
Rowkey : BaoXiao    Familiy:Quilifier : programming    Value : 99    Timestamp : 1627833364499
Rowkey : BaoXiao    Familiy:Quilifier : understanding    Value : 66    Timestamp : 1627833364499
Rowkey : Jack    Familiy:Quilifier : class    Value : 2    Timestamp : 1627833364261
Rowkey : Jack    Familiy:Quilifier : student_id    Value : 20210000000003    Timestamp : 1627833364261
Rowkey : Jack    Familiy:Quilifier : programming    Value : 80    Timestamp : 1627833364433
Rowkey : Jack    Familiy:Quilifier : understanding    Value : 80    Timestamp : 1627833364433
Rowkey : Jerry    Familiy:Quilifier : class    Value : 1    Timestamp : 1627833364227
Rowkey : Jerry    Familiy:Quilifier : student_id    Value : 20210000000002    Timestamp : 1627833364227
Rowkey : Jerry    Familiy:Quilifier : programming    Value : 67    Timestamp : 1627833364391
Rowkey : Jerry    Familiy:Quilifier : understanding    Value : 85    Timestamp : 1627833364391
Rowkey : Rose    Familiy:Quilifier : class    Value : 2    Timestamp : 1627833364293
Rowkey : Rose    Familiy:Quilifier : student_id    Value : 20210000000004    Timestamp : 1627833364293
Rowkey : Rose    Familiy:Quilifier : programming    Value : 61    Timestamp : 1627833364466
Rowkey : Rose    Familiy:Quilifier : understanding    Value : 60    Timestamp : 1627833364466
Rowkey : Tom    Familiy:Quilifier : class    Value : 1    Timestamp : 1627833364193
Rowkey : Tom    Familiy:Quilifier : student_id    Value : 20210000000001    Timestamp : 1627833364193
Rowkey : Tom    Familiy:Quilifier : programming    Value : 82    Timestamp : 1627833364358
Rowkey : Tom    Familiy:Quilifier : understanding    Value : 75    Timestamp : 1627833364358
```
