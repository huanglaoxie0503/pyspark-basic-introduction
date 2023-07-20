PySpark DataFrame

## 1. DataFrame简介

- 分布式数据容器,具有架构信息
- 类似关系型数据库表,或Pandas DataFrame
- 将数据分布在多个节点进行并行处理

```python
# 创建DataFrame的方法:
df = spark.read.json("data.json") 
df2 = spark.createDataFrame(data, schema)
```

## 2. DataFrame transformation

- filter、groupby、join等关系操作

```python  
df.filter(df.age > 20) # 过滤
df.groupBy("dept").avg() # 按部门分组聚合
df.join(df2, df.id == df2.id) # join连接
```

## 3. action操作

- 触发任务执行,如count、collect

```python
df.count() # 计数
df.collect() # 收集到驱动器
```

## 4. 数据源操作

- 读取和写入各种格式的数据源  

```python
df = spark.read.json("data.json") # 读JSON文件
df.write.saveAsTable("people_table") # 写入Hive表
```

## 5. 与RDD互操作

- RDD转换为DataFrame加入结构信息

```python
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF() # RDD转为DataFrame
```

- DataFrame转换为RDD进行低级处理

```python 
df.rdd.map(lambda row: row.age) # 访问RDD
```

## 6. 参数及性能优化

- 设置分区数,压缩编码,数据倾斜优化等

```python
df = spark.read.option("compression", "snappy") # 设置压缩
``` 

- 使用Catalanyst优化执行计划

Please let me know if you need any clarification or expansion of the contents!