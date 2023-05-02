## Overview

In this script, we first define the Kafka, MongoDB, and PySpark configuration properties.

Next, we define the MongoDB client object, which is used to connect to MongoDB and load data into it.

We then define the PySpark dataframe schema and the data transformation function, which filters data based on a condition.

We define the Kafka consumer object, which is used to extract data from Kafka.

We also define the PySpark dataframe reader and writer objects, which are used to read and write data to and from MongoDB.

Finally, we define the ETL pipeline, which extracts data from Kafka, transforms it using PySpark, and loads it into MongoDB.

This script can be used to build an ETL pipeline that extracts data from Kafka, transforms it using PySpark, and loads it into MongoDB, providing a scalable and flexible data processing architecture.
