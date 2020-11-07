# <center>Report for MapReduce2 Java</center>
<center>HAN Duqing & GAO Xin</center>

## 1.8 Remarkable trees of Paris
- List all jobs.

![](https://www.showdoc.com.cn/server/api/attachment/visitfile/sign/5f16e09277649a6d741bd4aebe34cf74?showdoc=.jpg)
### 1.8.1 Districts containing trees
A map/reduce program that lists the districts in the trees.csv.

| |  Mapper  | Reducer  |
|-|-|-|
|   ClassName  | DistrictsMapper  | DistrictsReducer |
|  InputKeyFormat   | LongWritable  | Text |
|  InputValueFormat   | Text  | NullWritable |
|  OutputKeyFormat   | Text  | Text |
|  OutputValueFormat   | NullWritable  | NullWritable |

- Run the job "districts".
```
yarn jar q7.jar districts trees.csv q1
```
![](https://www.showdoc.com.cn/server/api/attachment/visitfile/sign/3765cd0501741a8c6154d2a46fe2c0f4?showdoc=.jpg)

- Display the result of job "districts".
```
hdfs dfs -cat q1/part-r-00000
```
![](https://www.showdoc.com.cn/server/api/attachment/visitfile/sign/acf1c778fbd30a80d8f40e43d47b7a1f?showdoc=.jpg)

### 1.8.2 Show all existing species
A map/reduce program that lists the species in the trees.csv.

| |  Mapper  | Reducer  |
|-|-|-|
|   ClassName  | SpeciesMapper  | SpeciesReducer |
|  InputKeyFormat   | LongWritable  | Text |
|  InputValueFormat   | Text  | NullWritable |
|  OutputKeyFormat   | Text  | Text |
|  OutputValueFormat   | NullWritable  | NullWritable |

- Run the job "species".
```
yarn jar q7.jar species trees.csv q2
```
![](https://www.showdoc.com.cn/server/api/attachment/visitfile/sign/e4becae3ac758f310c37334e082a7130?showdoc=.jpg)

- Display the result of job "species".
```
hdfs dfs -cat q2/part-r-00000
```
![](https://www.showdoc.com.cn/server/api/attachment/visitfile/sign/3d6aa77fe77a15a0ed645b25f40d5444?showdoc=.jpg)

### 1.8.3 Number of trees by species
A map/reduce program that counts the species in the trees.csv.

| |  Mapper  | Reducer  |
|-|-|-|
|   ClassName  | SpecieCountMapper  | SpecieCountReducer |
|  InputKeyFormat   | LongWritable  | Text |
|  InputValueFormat   | Text  | IntWritable |
|  OutputKeyFormat   | Text  | Text |
|  OutputValueFormat   | IntWritable  | IntWritable |

- Run the job "speciecount".
```
yarn jar q7.jar speciecount trees.csv q3
```
![](https://www.showdoc.com.cn/server/api/attachment/visitfile/sign/f279c29a230781348c2d0249e1f8db95?showdoc=.jpg)

- Display the result of job "speciecount" (key-value: specie-count).
```
hdfs dfs -cat q3/part-r-00000
```
![](https://www.showdoc.com.cn/server/api/attachment/visitfile/sign/76671962b8e3e5b9ec2c195f97f7bae7?showdoc=.jpg)

### 1.8.4 Maximum height per specie of tree
A map/reduce program that calculates the height of the tallest tree of each specie in the trees.csv.

| |  Mapper  | Reducer  |
|-|-|-|
|   ClassName  | SpecieMaxHeightMapper  | SpecieMaxHeightReducer |
|  InputKeyFormat   | LongWritable  | Text |
|  InputValueFormat   | Text  | FloatWritable |
|  OutputKeyFormat   | Text  | Text |
|  OutputValueFormat   | FloatWritable  | FloatWritable |

- Run the job "speciemaxheight".
```
yarn jar q7.jar speciemaxheight trees.csv q4
```
![](https://www.showdoc.com.cn/server/api/attachment/visitfile/sign/9ca9181f736b4f7d5a06f2d04e7da3f3?showdoc=.jpg)

- Display the result of job "speciemaxheight" (key-value: specie-maxheight).
```
hdfs dfs -cat q4/part-r-00000
```
![](https://www.showdoc.com.cn/server/api/attachment/visitfile/sign/6da604a4974586a8239b4c400a084449?showdoc=.jpg)

### 1.8.5 Sort the trees height from smallest to largest
A map/reduce program that sorts the trees height from smallest to largest in the trees.csv.

| |  Mapper  | Reducer  |
|-|-|-|
|   ClassName  | SortHeightMapper  | SortHeightReducer |
|  InputKeyFormat   | LongWritable  | FloatWritable |
|  InputValueFormat   | Text  | Text |
|  OutputKeyFormat   | FloatWritable  | FloatWritable |
|  OutputValueFormat   | Text  | Text |

- Run the job "sortheight".
```
yarn jar q7.jar sortheight trees.csv q5
```
![](https://www.showdoc.com.cn/server/api/attachment/visitfile/sign/78d97460dbee482825863b44353d3470?showdoc=.jpg)

- Display the result of job "sortheight" (key-value: height-objectid).
```
hdfs dfs -cat q5/part-r-00000
```
![](https://www.showdoc.com.cn/server/api/attachment/visitfile/sign/62bd40310da683585996b439e82e4eb0?showdoc=.jpg)

### 1.8.6 District containing the oldest tree
A map/reduce program that displays the district where the oldest tree is in the trees.csv.

We define a Writable class called MyWritable which is the pair of (plantyear , district).

| |  Mapper  | Reducer  |
|-|-|-|
|   ClassName  | OldestTreeMapper  |  OldestTreeReducer |
|  InputKeyFormat   | LongWritable  | Text |
|  InputValueFormat   | Text  | MyWritable |
|  OutputKeyFormat   | Text  | Text |
|  OutputValueFormat   | MyWritable  | NullWritable |

- Run the job "oldesttree" and display the result.
```
yarn jar q7.jar oldesttree trees.csv q6
hdfs dfs -cat q6/part-r-00000
```
![](https://www.showdoc.com.cn/server/api/attachment/visitfile/sign/e2adbd340ca8c62505df262a093c7996?showdoc=.jpg)

- Verify the result of job "oldesttree".
![](https://www.showdoc.com.cn/server/api/attachment/visitfile/sign/ce073cb0ac87c20d4377a5f2310eb1f7?showdoc=.jpg)
We can see that District 5 have the oldest tree is correct.

### 1.8.7 District containing the most trees
A map/reduce program that displays the district that contains the most trees in the trees.csv.

We define a Writable class called MyWritable which is the pair of (district , count).

We also create 2 MapReduce jobs to handle this question.

| |  Mapper1  | Reducer1  |  Mapper2  | Reducer2  |
|-|-|-|-|-|
|   ClassName  | MostTreeMapper1  |  MostTreeReducer1 |MostTreeMapper2  |  MostTreeReducer2 |
|  InputKeyFormat   | LongWritable  | Text | Oject  |  NullWritable |
|  InputValueFormat   | Text  | IntWritable |Text  |  MyWritable |
|  OutputKeyFormat   | Text  | Text |NullWritable  |  Text |
|  OutputValueFormat   | IntWritable  | IntWritable |MyWritable | NullWritable |

- Run the job "mosttree" and display the result.
```
yarn jar q7.jar mosttree trees.csv q7
hdfs dfs -cat q7/part-r-00000
```
![](https://www.showdoc.com.cn/server/api/attachment/visitfile/sign/d0b7441713f742b072b64bfc7ebab86b?showdoc=.jpg)

- Verify the result of job "oldesttree".
![](https://www.showdoc.com.cn/server/api/attachment/visitfile/sign/e36856952969b42b669408d232e0cc1f?showdoc=.jpg)
We can see that District 16 have most trees is correct.