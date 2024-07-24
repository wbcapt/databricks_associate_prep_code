#Create a schema

from pyspark.sql.types import structType, StructField, StringType, IntegerType,DateType,FloatType,DoubleType

<your_schema_name> = structType ([
    StructField('FieldName1111', StringType())
    ,StructField('FieldName2222', IntegerType())
])


#Create a dataframe to read a csv file

df= (spark.read.format('csv')
    .option('header','true')
    .schema(<your_schema_name>)
    .load(f'{<file_source>}/<directory_folder>/*.csv)
)


#Create a Source Table

CREATE TABLE <your_table_name>
(
    Column_name_1 VARCHAR(50)
    ,Column_name_2 INT
)



#Upsert (overwrite matching rows with new updates + append any non-matching rows)

MERGE INTO <destination_table>
USING <source_table>

ON 
<dest.col1> = <src.col1>
,<dest.col2> = <src.col2>

WHEN MATCHED
THEN UPDATE SET
    <dest.col1> = <src.col1>
    ,<dest.col2> = <src.col2> 

WHEN NOT MATCHED
THEN INSERT
    VALUES(<src.Col1>,<src.Col2>)


================================================================
#Structured Streaming
==================================================================

#1 Create a dataframe
%PYTHON
from pyspark.sql.types import structType, StructField, StringType, IntegerType,DateType,FloatType,DoubleType

<your_schema_name> = structType ([
    StructField('FieldName1111', StringType())
    ,StructField('FieldName2222', IntegerType())
])

#2 create a schema using SQL
%SQL
CREATE SCHEMA IF NOT EXISTS <your_schema_name>;
use Stream

#3 create a dataframe
df = spark.readStream.format("csv")\
    .option('header','true')\
    .schema(schema)\
    .loag(<source_directory>)

#4 write a datefrmae to a table
<stream_name> = (df.<stream_name>
    .option('checpointLocation',f'{<source_dir>}/AppendCheckpoint)
    .outputMode('append')
    .queryName('<your_query_name>')
    .toTable('<schema>.<table_name>')
)

#5 Query a table
SELECT * FROM <schema>.<stream_table_name>

#6 Stop Stream
<stream_name>.stop()