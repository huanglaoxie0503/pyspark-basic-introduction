## 1. PySpark SQL概述

- PySpark提供了Python API来使用Spark SQL的功能
- 可以通过SparkSession实例进行交互

    ```python
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("sql").getOrCreate()
    ```

- 支持访问结构化数据源,分析和查询

    ```python 
    df = spark.read.json("people.json")
    df.filter(df.age > 20).show()
    ```

## 2. DataFrame操作

- 创建DataFrame的常见方式

    ```python
    df = spark.read.csv("data.csv") # 从CSV文件创建
    df2 = spark.createDataFrame(data, schema) # 从Python数据创建
    ```

- 常见的DataFrame操作

    ```python
    df.groupby("dept").count().show() # 分组聚合
    df.join(df2, "id").show() # 表连接 
    ```

## 3. 使用SQL查询

- 通过spark.sql方法执行SQL

    ```python
    spark.sql("SELECT * FROM TABLE WHERE age > 20").show()
    ```

- 支持UDF用户自定义函数

    ```python
    spark.udf.register("strLen", lambda x: len(x))  
    spark.sql("SELECT strLen('foo')").show()
    ```

## 4. 数据源访问

- 内置对多种数据格式的访问和保存支持

    ```python
    df.write.json("output") # 写入JSON文件
    df = spark.read.parquet("table.parquet") # 读Parquet文件
    ```

## 5. 与pandas集成

- Pandas与Spark DataFrame转换

    ```python
    pdf = pd.DataFrame(data) 
    df = spark.createDataFrame(pdf) 
    pdf = df.toPandas()
    ```

- 利用pandas进行数据处理再传入Spark

## 6. 多语言支持

- PySpark可与Scala, Java, R等语言混合使用

    ```python
    # 调用Scala用户定义函数
    df.withColumn("newCol", callUDF("scalaUDF")) 
    ```

- 在一个应用中混合语言进行开发

## 7. 视图和临时表

- 注册临时表进行SQL查询

    ```python
    df.createOrReplaceTempView("people")
    spark.sql("SELECT * FROM people").show()
    ```

- 全局临时视图在会话间共享

    ```python
    df.createGlobalTempView("people")
    ```