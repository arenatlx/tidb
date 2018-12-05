### Hackathon 2018脚本




#### 需要在pg中，和当前的tidb中都创建模式


`create table remoteredis(k  varchar(20), v varchar(20));`

`create table remotepg(id int, name varchar(20), age int);`

`create table remotepg2(id int, title varchar(20), number int);`

`create table remotecsv(id int, price int);`


`insert into foreign_register values(“remoteredis”,”redis”, “127.0.0.1#5435”);`

`Insert into foreign_register values(“remotepg”, “postgresql”, “127.0.0.1#5434#remotepg”);`

`Insert into foreign_register values(“remotepg2”, “postgresql”, “127.0.0.1#5434#remotepg2”)`

`Insert into foreign_register values(“remotecsv”, “csv”, “127.0.0.1#5433#test.csv”);`

#### Attention:
需要先将三个rpc服务器开启，对应的postgresql和redis服务要启动，csv源文件的路径要使用正确
可以写边rpc的test的进行rpc服务的测试

之后开启tidb，连接到mysql client，进入到mysql的系统库目录下，进行以上6条脚本的处理，然后就行
查外源表了
