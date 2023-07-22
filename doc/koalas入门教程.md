# Koalas入门教程

## Koalas简介

Koalas是基于Apache Spark的Python API,提供与pandas类似的DataFrame API。使用Koalas可以让数据科学家在大数据环境下,通过熟悉的pandas代码进行分布式数据处理。

Koalas DataFrame的底层实现连接到Spark DataFrame,可以互相转换。计算通过延迟评估实现懒执行。

## 安装配置

安装Koalas:

```
pip install databricks-koalas
```

需要Python 3.5+环境。

启动Spark Session:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("koalas_test").getOrCreate() 
```

导入koalas库:

```python 
import databricks.koalas as ks
```

## Koalas中常见的基础DataFrame操作

### 创建DataFrame

- 从Python dict/list创建

```python
data = {'a':[1,2], 'b':[3,4]}
df = ks.DataFrame(data)
```

- 从文件(CSV, JSON, Parquet等)创建 

```python
df = ks.read_csv('data.csv')
```

- 从SQL表和数据库创建

```python
df = ks.read_sql('SELECT * FROM table', conn)
```

### 查看和描述

- 查看前几行 `.head()`

- 查看数据概述 `.describe()` 

- 查看索引:`.index`

- 查看列:`.columns`

### 选择数据

- 列选择:`df[['col1','col2']]`

- 行选择:`df[df['col1']>0]`

- 采样:`df.sample(n)`

### 基本操作

- 列映射:`df['new'] = df['old']` 

- 添加列:`df['new_col'] = [1,2,3]`

- 排序:`df.sort_values('col1')`

- 过滤行:`df = df[df['col']>0]` 

- 分组聚合:`df.groupby('key').agg({'col1': 'mean'})`

Koalas支持类似pandas的基础DataFrame操作,可以快速上手进行分析。