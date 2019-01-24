# DataX KuduReader 插件文档


------------

## 1 快速介绍

KuduReader提供从kudu集群读取数据。在底层实现上，KuduReader通过impala JDBC连接远程kudu数据库，并执行相应的sql语句将数据从kudu库中SELECT出来。

**不同于其他关系型数据库，KuduReader不支持FetchSize.**

## 2 实现原理

简而言之，KuduReader通过Impala JDBC连接器连接到远程的kudu数据库，并根据用户配置的信息生成查询SELECT SQL语句，然后发送到远程kudu数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，KuduReader将其拼接为SQL语句发送到Mysql数据库；对于用户配置querySql信息，KuduReader直接将其发送到kudu数据库。


## 3 功能说明

### 3.1 配置样例

* 配置一个从kudu数据库同步抽取数据到mysql的作业:

```
{
  "job": {
    "setting": {
      "speed": {
        "channel": "5"
      }
    },
    "content": [
      {
        "reader": {
          "name": "kudureader",
          "parameter": {
            "username": "bigdata",
            "password": "bigdata",
            "connection": [
              {
                "querySql": [
                  "select user_id,account_id,dt,merchant_id,master_amount,slave_amount from account_balance;"
                ],
                "jdbcUrl": [
                  "jdbc:impala://172.17.10.63:21050/k_yc_ods;auth=noSasl"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "mysqlwriter",
          "parameter": {
            "column": ["user_id","account_id","dt","merchant_id","master_amount","slave_amount"],
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://172.17.1.188:3306/test?characterEncoding=utf8",
                "table": ["account_balance_test"]
              }
            ],
            "password": "Rosc37fh",
            "preSql": ["truncate account_balance_test"],
            "session": [],
            "username": "datax",
            "writeMode": "insert"
          }
        }
      }
    ]
  }
}

```

### 3.2 参数说明

* **jdbcUrl**

	* 描述：描述的是到对端数据库的JDBC连接信息，使用JSON的数组描述，并支持一个库填写多个连接地址。之所以使用JSON数组描述连接信息，是因为阿里集团内部支持多个IP探测，如果配置了多个，KuduReader可以依次探测ip的可连接性，直到选择一个合法的IP。如果全部连接失败，KuduReader报错。 注意，jdbcUrl必须包含在connection配置单元中。对于阿里集团外部使用情况，JSON数组填写一个JDBC连接即可。

		jdbcUrl按照impala官方规范，并可以填写连接附件控制信息。具体请参看[impala官方文档].

	* 必选：是 <br />

	* 默认值：无 <br />

* **username**

	* 描述：数据源的用户名 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **password**

	* 描述：数据源指定用户名的密码 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **table**

	* 描述：所选取的需要同步的表。使用JSON的数组描述，因此支持多张表同时抽取。当配置为多张表时，用户自己需保证多张表是同一schema结构，KuduReader不予检查表是否同一逻辑表。注意，table必须包含在connection配置单元中。<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。用户使用\*代表默认使用所有列配置，例如['\*']。

	  支持列裁剪，即列可以挑选部分列进行导出。

      支持列换序，即列可以不按照表schema信息进行导出。

	  支持常量配置，用户需要按照Mysql SQL语法格式:
	  ["id", "\`table\`", "1", "'bazhen.csy'", "null", "to_char(a + 1)", "2.3" , "true"]
	  id为普通列名，\`table\`为包含保留在的列名，1为整形数字常量，'bazhen.csy'为字符串常量，null为空指针，to_char(a + 1)为表达式，2.3为浮点数，true为布尔值。

	* 必选：是 <br />

	* 默认值：无 <br />

* **splitPk**

	* 描述：KuduReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片，DataX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能。

	  推荐splitPk用户使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。

	  目前splitPk仅支持整形数据切分，`不支持浮点、字符串、日期等其他类型`。如果用户指定其他非支持类型，KuduReader将报错！

	  如果splitPk不填写，包括不提供splitPk或者splitPk值为空，DataX视作使用单通道同步该表数据。

	* 必选：否 <br />

	* 默认值：空 <br />

* **where**

	* 描述：筛选条件，KuduReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > $bizdate 。注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句。<br />

          where条件可以有效地进行业务增量同步。如果不填写where语句，包括不提供where的key或者value，DataX均视作同步全量数据。

	* 必选：否 <br />

	* 默认值：无 <br />

* **querySql**

	* 描述：在有些业务场景下，where这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选SQL。当用户配置了这一项之后，DataX系统就会忽略table，column这些配置型，直接使用这个配置项的内容对数据进行筛选，例如需要进行多表join后同步数据，使用select a,b from table_a join table_b on table_a.id = table_b.id <br />

	 `当用户配置querySql时，KuduReader直接忽略table、column、where条件的配置`，querySql优先级大于table、column、where选项。

	* 必选：否 <br />

	* 默认值：无 <br />


### 3.3 类型转换

目前KuduReader支持大部分Kudu类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出KuduReader针对kudu类型转换列表:


| DataX 内部类型| kudu 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, bigint|
| Double   |float, double|
| String   |string|
| Date     |unixime_micros|
| Boolean  |bool   |
| Bytes    |binary    |


## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征
建表语句：

单行记录类似于：


#### 4.1.2 机器参数

* 执行DataX的机器参数为:
	1. cpu: 24核 Intel(R) Xeon(R) CPU E5-2630 0 @ 2.30GHz
	2. mem: 48GB
	3. net: 千兆双网卡
	4. disc: DataX 数据不落磁盘，不统计此项

* kudu数据库机器参数为:


#### 4.1.3 DataX jvm 参数

	-Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError


### 4.2 测试报告

#### 4.2.1 单表测试报告


| 通道数| 是否按照主键切分| DataX速度(Rec/s)|DataX流量(MB/s)| DataX机器网卡进入流量(MB/s)|DataX机器运行负载|DB网卡流出流量(MB/s)|DB运行负载|
|--------|--------| --------|--------|--------|--------|--------|--------|

说明：

1. 这里的单表，主键类型为 bigint(20),范围为：190247559466810-570722244711460，从主键范围划分看，数据分布均匀。
2. 对单表如果没有安装主键切分，那么配置通道个数不会提升速度，效果与1个通道一样。


#### 4.2.2 分表测试报告(2个分库，每个分库16张分表，共计32张分表)


| 通道数| DataX速度(Rec/s)|DataX流量(MB/s)| DataX机器网卡进入流量(MB/s)|DataX机器运行负载|DB网卡流出流量(MB/s)|DB运行负载|
|--------| --------|--------|--------|--------|--------|--------|

## 5 约束限制

### 5.1 数据库编码问题

kudu本身的编码设置非常灵活，包括指定编码到库、表、字段级别，甚至可以均不同编码。优先级从高到低为字段、表、库、实例。我们不推荐数据库用户设置如此混乱的编码，最好在库级别就统一到UTF-8。

kuduReader底层使用impala JDBC进行数据抽取，JDBC天然适配各类编码，并在底层进行了编码转换。因此KuduReader不需用户指定编码，可以自动获取编码并转码。

对于kudu底层写入编码和其设定的编码不一致的混乱情况，KuduReader对此无法识别，对此也无法提供解决方案，对于这类情况，`导出有可能为乱码`。

### 5.4 增量数据同步

KuduReader使用JDBC SELECT语句完成数据抽取工作，因此可以使用SELECT...WHERE...进行增量数据抽取，方式有多种：

* 数据库在线应用写入数据库时，填充modify字段为更改时间戳，包括新增、更新、删除(逻辑删)。对于这类应用，KuduReader只需要WHERE条件跟上一同步阶段时间戳即可。
* 对于新增流水型数据，KuduReader可以WHERE条件后跟上一阶段最大自增ID即可。

对于业务上无字段区分新增、修改数据情况，KuduReader也无法进行增量数据同步，只能同步全量数据。

### 5.5 Sql安全性

KuduReader提供querySql语句交给用户自己实现SELECT抽取语句，KuduReader本身对querySql不做任何安全性校验。这块交由DataX用户方自己保证。

## 6 FAQ

***

**Q: KuduReader同步报错，报错信息为XXX**


