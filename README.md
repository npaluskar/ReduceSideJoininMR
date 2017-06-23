# ReduceSideJoininMR
Performing Reduce Side Join in Java Mapreduce

This code performs Reduce Side join of folowing two files to find out total_sales by state. Both the files are tab delimited. 

Example data set

Customers
customer_id	customer_name	zip_code	City	state

Sales
customer_id	sales_price	timestamp

Output Expected 
state total_sales

Steps for running the Code.
1. javac -classpath ${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/common/hadoop-common-${HADOOP_VERSION}.jar -d ${HOME}/reducejoin_assignment TotalSalesByState.java

2. cd ..

3. jar -cvf totalsalesbystate.jar -C ${HOME}/reducejoin_assignment/ .

4. hadoop jar totalsalesbystate.jar TotalSalesByState /hdfs_file/customers /hdfs_file/sales /hdfs_file/total_sales

5. hadoop fs -rmr  /hdfs_path/intermediate_output (For next run)

Similar Hive Query for this Mapreduce Code would be

select state,count(*) from customers group by state;



