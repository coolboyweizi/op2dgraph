<pre>
Dgraph参考文档: https://docs.dgraph.io/

oplog2dgraph: 根据mongodb数据关系添加到dgraph数据库
代码主要是依赖oplog来识别数据关系变动，因为oplog会自动覆盖之前的数据。
所以运行方案是先同步后增量

代码结构
|-- cols            // mongodb的集合关系转化
|
|-- data            // 增量同步数据存储。
|
|-- logs            // 程序输出日志文件，如果报错是err文件
|
|-- module          // oplog读取与dgraph存储
|
|-- app.py          // 启动程序
|
|-- config.py       // 配置文件
|
|-- initSchema.py   // 初始化DATA目录下的rdf文件


同步操作:  service中运行4个docker。

// data文件夹生成压缩好的文件 goldendata.rdf.gz，并复制到docker对应的根目录下
python initSchema.py  
         
// 停止drgaph-server端
docker stop dgraph-server-0 

// 运行导入数据服务
docker start dgraph-bulk

// 在根目录下找到out并把数据还原对应的目录并覆盖。启动server
docker start dgraph-server-0 

// 最后运行该程序实现增量同步。该程序增量同步的节点从今日凌晨开始


</pre>