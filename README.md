# PySpark Classes

##  PySpark 

- PySpark is the Python API for Apache Spark that lets Python users run distributed data processing and analytics on large datasets. PySpark provides libraries for working with DataFrames, running SQL like queries and building machine learning workflows using familiar Python code.
  
- https://www.databricks.com/blog/what-is-pyspark#:~:text=PySpark%20is%20the%20Python%20API,workflows%20using%20familiar%20Python%20code.

- Key ConceptsDistributed Computing: PySpark takes your Python code and translates it into tasks executed across a cluster of machines or multiple local cores
- DataFrames: The primary API for working with structured data, making it easy to filter, group, and aggregate data similar to pandas but on a much larger scale
- Lazy Evaluation: Operations are recorded in an execution plan but are only computed when an "action" (such as .show() or .collect()) is triggered.

- https://pypi.org/project/pyspark/
- https://www.databricks.com/blog/what-is-pyspark
- https://spark.apache.org/docs/latest/api/python/index.html
- https://github.com/apache/spark/blob/master/python/pyspark/sql/dataframe.py
- https://github.com/topics/pyspark?o=desc&s=stars
  
---

## What is Apache Spark™?
- Apache Spark™ is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters.

- https://github.com/apache/spark

- https://spark.apache.org/
  
---

# pyspark and apache spark are same

- No, PySpark and Apache Spark are not the exact same thing, but they are deeply connected. Apache Spark is the core open-source distributed computing engine, while PySpark is the official Python API interface created to control that engine using Python code
- Think of Apache Spark as the car's engine, and PySpark as the specific steering wheel and dashboard tailored for Python drivers.

## Core Differences

- What it is: Apache Spark is the entire big data processing framework, originally written in Scala. PySpark is a wrapper library that translates Python commands into a language the Spark core engine understands
- Language Support: Apache Spark natively supports multiple programming languages, including Scala, Java, Python, and R. PySpark is exclusively used for Python programming.
- Target Audience: Apache Spark (via Scala/Java) is traditionally favored by data engineers building massive, performance-critical backend pipelines. PySpark is the go-to tool for data scientists and analysts who want to scale up their machine learning and data analysis pipelines using pythonic toolsets like pandas and NumPy

<img width="1312" height="718" alt="image" src="https://github.com/user-attachments/assets/cb4793ce-c8d4-4597-9072-312fcc25fdb5" />

---

## MLlib
- MLlib is a wrapper over the PySpark and it is Spark’s machine learning (ML) library. This library uses the data parallelism technique to store and work with data. The machine-learning API provided by the MLlib library is quite easy to use. MLlib supports many machine-learning algorithms for classification, regression, clustering, collaborative filtering, dimensionality reduction, and underlying optimization primitives.

---

<img width="1624" height="1262" alt="image" src="https://github.com/user-attachments/assets/121eca71-81ec-43bf-8489-70076935e034" />

---
