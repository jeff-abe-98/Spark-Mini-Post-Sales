from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType


if __name__ == '__main__':
    spark = SparkSession.builder.appName('PostSalesReport').getOrCreate()

    schema = StructType([
        StructField('incident_id',IntegerType(),True),
        StructField('incident_type', StringType(), True),
        StructField('vin_number', StringType(), True),
        StructField('make', StringType(), True),
        StructField('model', StringType(), True),
        StructField('year', StringType(), True),
        StructField('incident_date', DateType(), True),
        StructField('description', StringType(), True)
    ])

    raw_frame = spark.read.format('csv').schema(schema).load('/mnt/mini-proj/data.csv')

    def extract_vin_key_value(x):
        '''
        Function to extract the vin as the key, and assemble a value list

        Args:
            x: row passed through map function
        
        returns
            tuple: tuple of key, value pair
        '''
        r_tuple = (x['vin_number'], [x['make'], x['year'],x['incident_type']]) if x['incident_type'] == 'I' else (x['vin_number'], [None,None,x['incident_type']])
        return r_tuple

    vin_kv = raw_frame.rdd.map(lambda x: extract_vin_key_value(x))

    def populate_make(iter):
        '''
        Function to populate the make and year across rows. Rows are already grouped by a key, and only need to have the make and year propagated to all other rows.

        Args:
            iter: ResultIterable that was passed from groupByKey()

        Returns:
            list: list of make, year values for each item in the passed iterable
        '''
        l = [item[0:2] for item in iter if item[0] is not None]
        return l*len(iter)

    enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))

    def extract_make_key_value(x):
        '''
        Function to create key value pairs for reduction. Assembles a string, and sets the value as 1.

        Args:
            x: key value pair from the source rdd

        Return:
            tuple: A tuple of key, value pairs
        '''
        return (x[0]+'-'+x[1], 1)

    make_kv = enhance_make.map(lambda x: extract_make_key_value(x))

    # Reducing by make and year to get the total number of records grouped by make and year
    final = make_kv.reduceByKey(lambda x, y: x + y)

    # Saving final file as a csv
    final.toDF().write.csv('/mnt/mini-proj/output.csv')

    spark.stop()