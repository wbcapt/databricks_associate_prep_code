#==============================Table & Schema Operations==============================


#+++++++++++++++++++++++++++++Creates+++++++++++++++++++++++++++++++++

#Write the pyspark code for creating a schema
from pyspark.sql.types import structType, StructField, StringType, IntegerType,DateType,FloatType,DoubleType

<your_schema_name> = structType ([
    StructField('FieldName1111', StringType())
    ,StructField('FieldName2222', IntegerType())
])

#write the sparksql code for creating a schema
CREATE SCHEMA IF NOT EXISTS buster_schema_default_location


#write the spark sql for creating a table
CREATE TABLE IF NOT EXISTS <schema_name>.<your_table_name>
(
        Field_1 String
        ,Field_2 INT
)

#write the spark SQL for deleting a record for a table
DELETE FROM <schema>.<my_table>
WHERE <field_name> = '<value>'


#write the code for creating a temp view
<df_name>.createOrReplaceTempView('<your_view_name>')

#Write the SPARK sql to change a view

spark.sql("""   UPDATE ParquetView SET Education_Level = 'School' WHERE Education_Level = 'High School'  """)

#Write the code for (Q24) select records with less than a condition (filter)

#write the pyspark code for defining a schema

#Write the code for creating a database

#Write the code for creating a table

#Write a CTAS statement for creating a delta table

#Write a CTAS Statement for creating a delta table from an existing PostgreSQL Database

#Write a CTAS statement for creating a table from a query with a comment


#+++++++++++++++++++++++++++++UPDATES++++++++++++++++++++++++++++++++++++++++++

#Write the code to delete a row from a table

#write the SQL code to change a record in a view or table
UPDATE <view_name> or <my_schema>.<my_table>
SET <field_name> = '<new_field_value>'
WHERE <field_name> = '<current_field_value'

UPDATE ParquetView 
SET Education_Level = 'School'
WHERE Education_Level = 'High School'

#write the SQL Code to insert records into a delta table
INSERT INTO <my_schema>.<my_Table_name>
VALUES
        ('field1_val', 'field2_val')
        ,('field1_val', 'field2_val');



#==================Dataframe Operations=========================================

#write the code to configure a schema
from pyspark.sql.types import StructType, StructField, StringType , IntegerType, FloatType

schema = StructType([   
                     StructField('Country',StringType()),
                     StructField('Citizens',IntegerType())
])

#write the pyspark code for creating a dataframe for CSV
df = (spark.read.format('csv')
            .option('header','true')
            .schema(<your_schema_name>)
            .load(f'{source}/<dir>/*.csv')
)

#write the pyspark code for creating a dataframe for parquet
df = (spark.read.format('parquet')
                .load(f'{source}/<dir>/<file_name>.parquet')
)

#write the code to view a schema
df.printSchema()

#write the pyspark_code for creating a table from a CSV File

#write the pyspark code for filtering a dataframe
<your_df> = <your_df>.filter("<field_name> == '<field_value'>")

#write the pyspark code for overwriting a file
(your_df.write.format('parquet')
    .mode('overwrite')
    .save(f'{source}/<your_directory>/'))

(df_parquet.write.format('parquet')
    .mode('overwrite')
    .save(f'{source}/Temp/'))

#write the pyspark code for creating a delta lake
(df.write.format('delta')
    .mode('overwrite')
    .save(f'{source}/<your_directory>/'))







#write the code for reading a dataframe

#Write the code for showing the contents of a dataframe


#what is the spark command to query a table?
spark.table("employees")

#write the code for a user defined function

#write the code for creating a spark structured stream table

#write the code to stop a spark structured stream



CREATE SCHEMA IF NOT EXISTS  stream;
use stream

#write the code to create a dataframe and read a stream
df = spark.readStream.format("csv")\
        .option('header','true')\
        .schema(<your_schema_name>)\
        .load(your_source_dir)


#write a stream with a checkppint
<streamingDataframe>.writeStream.option('checkpointLocation',<location>).outputMode('append').toTable('<your_table_name>')

 WriteStream = ( df.writeStream
        .option('checkpointLocation',f'{source_dir}/AppendCheckpoint')
        .outputMode("append")
        .queryName('AppendQuery')
        .toTable("stream.AppendTable"))


#write the checkpoint
