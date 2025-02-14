# Lab 6

- **Full Name:** Arun Chacko  
- **E-mail:** [achac021@ucr.edu](mailto:achac021@ucr.edu)  
- **UCR NetID:** achac021  
- **Student ID:** 862298328  

---

### **(Q1) What are these two arguments?**
The two arguments are:
1. **Command** – Specifies the operation that the code should perform.
2. **Input File** – The path to the input file containing the data to be processed.

---

### **(Q2) What is the type of the attributes `time` and `bytes` this time? Why?**
- **Before** commenting out `option("inferSchema", "true")`, the types of `time` and `bytes` are **integers**.
- **After** commenting out the line, the types change to **strings**.

---

### **(Q3) How many jobs does your program have? List them and describe each.**

My program has **3 jobs**:
1. **countByKey** – Counts each element for each key and returns the total count.
2. **sortByKey** – Sorts the response codes by key in ascending order.
3. **collect** – Returns an array containing all elements from `sortByKey`.

---

### **(Q4) How many stages does your program have? Why does `countByKey` have two stages?**
My program has a total of **6 stages** across the three jobs:
- **Job 0 (countByKey)** → 2 stages
- **Job 1 (sortByKey)** → 2 stages
- **Job 2 (collect)** → 2 stages (1 skipped)

The **countByKey** function has two stages because it involves a **shuffle operation**:
- **Stage 1:** The filter and map transformations occur.
- **Stage 2:** The shuffle operation redistributes the data.

---

### **(Q5) What are the longest two stages? Why?**
The longest two stages are:
1. **The map function (`countByKey`)**
2. **Stage 1 of `sortByKey`**

These stages are the longest because they process large amounts of data:
- `countByKey` is particularly slow due to **filtering** and **shuffling**.
- `sortByKey` takes time to rearrange the dataset in ascending order.

---

### **(Q6) Copy the output of the command including the code, Avg(bytes) table, and runtime.**
```sh
cs167@class-219:~/cs167$ spark-submit --class edu.ucr.cs.cs167.achac021.App \
--master spark://class-211:7077 achac021_lab6-1.0-SNAPSHOT.jar avg-bytes-by-code nasa_19950630.22-19950728.12_achac021.tsv
```
**Output:**
```
Using Spark master 'spark://class-211:7077'
Average bytes per code for the file 'nasa_19950630.22-19950728.12_achac021.tsv'

Code    Avg(bytes)
200     22739.652244386536
302     79.0597341807485
304     0.0
403     0.0
404     0.0
500     0.0
501     0.0

Command 'avg-bytes-by-code' on file 'nasa_19950630.22-19950728.12_achac021.tsv' finished in **116.443268312 seconds**
```

---

### **(Q7) How many jobs does your program have? How does it compare to the previous program?**
My program has **4 jobs**:
1. **load**
2. **show**
3. **collect**
4. **collect**

Compared to the previous program, this one has **shorter job durations**. The shorter duration could be due to:
- A **smaller dataset**
- **Less complex operations**
- The `show()` and `collect()` jobs mainly retrieving data for **local processing**

---

### **(Q8) How many stages does your program have? How does this affect runtime?**
My program has **four jobs, each with one stage**. Even though there are fewer stages, the runtime did not change significantly. 

---

### **(Q9) SQL Execution Plan Analysis**
Visit this link: [http://localhost:4040/SQL](http://localhost:4040/SQL). 
1. Click on **`collect at AppSQL`** to view the query execution graph.
2. Scroll to the bottom and click on **`> Details`**.
3. Copy the execution plans below and analyze:

```
== Physical Plan ==
AdaptiveSparkPlan (16)
+- == Final Plan ==
   * Sort (10)
   +- AQEShuffleRead (9)
      +- ShuffleQueryStage (8), Statistics(sizeInBytes=96.0 B, rowCount=4)
         +- Exchange (7)
            +- * HashAggregate (6)
               +- AQEShuffleRead (5)
                  +- ShuffleQueryStage (4), Statistics(sizeInBytes=128.0 B, rowCount=4)
                     +- Exchange (3)
                        +- * HashAggregate (2)
                           +- Scan csv  (1)
+- == Initial Plan ==
   Sort (15)
   +- Exchange (14)
      +- HashAggregate (13)
         +- Exchange (12)
            +- HashAggregate (11)
               +- Scan csv  (1)
```
#### **Analysis**
- The final plan is Spark's reoptimized plan using runtime stats so therefore it is the more optimal one.
---
