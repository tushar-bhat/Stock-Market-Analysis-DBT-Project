## Requirements
### The brackets show the versions used in this project.
* Java (17.0.6)
* Scala (2.12)
* Python (3.10)
* Xampp Server (8.2.4-0)
* Spark (3.1.2)
* Zookeeper (3.8.0)
* Kafka (3.4.0)
* Python modules to pip install: streamlit, matplotlib, mysql-connector-python, dotenv, kafka-python, pyspark, pandas.

## Running

Head over to https://site.financialmodelingprep.com/developer/docs, create an account and go to the dashboard to get your API key. You will need 5 API keys, so repeat the process with 4 more gmail accounts. 
In the repository folder, create a file called `.env` and put
```
API_KEY_1 = "<your_first_api_key>"
API_KEY_2 = "<your_second_api_key>"
API_KEY_3 = "<your_third_api_key>"
API_KEY_4 = "<your_fourth_api_key>"
API_KEY_5 = "<your_fifth_api_key>"
```

Never upload any of these keys onto github.

Assuming zookeeper and kafka are properly configured and added to path in .bashrc, run `zookeeper-server-start.sh <path_to_zookeeper_installation>/kafka_2.12-3.4.0/config/zookeeper.properties`, replace <path_to_zookeeper_installation> with the path of the zookeeper installation. In the above command, regarding kafka_2.12-3.4.0, 2.12 is the Scala version and 3.4.0 is the kafka version.

Then in a new terminal run `kafka-server-start.sh <path_to_kafka_installation>/kafka_2.12-3.4.0/config/server.properties`. 
Make sure to never close the terminals with zookeeper and kafka.

Assuming that xampp has been installed into the default directory /opt, run `sudo /opt/lampp/xampp start`.

Open ur browser and head to localhost/phpmyadmin to make sure the MySQL server is running.

Create a database 'stock' and create two tables:
* simple_data - symbol varchar(4), name varchar(30), price float, volume int, tstamp timestamp.
* agg_data - symbol varchar(4), start_time timestamp, end_time timestamp, price_avg float, price_min float, price_max float, volume_avg float, volume_min int, volume_max int, cnt int.

```
CREATE TABLE `stock`.`simple_data` (`symbol` VARCHAR(4) NOT NULL , `name` VARCHAR(30) NOT NULL , `price` FLOAT NOT NULL , `volume` INT NOT NULL , `tstamp` TIMESTAMP NOT NULL ) ENGINE = InnoDB; 

CREATE TABLE `stock`.`agg_date` (`symbol` VARCHAR(4) NOT NULL , `start_time` TIMESTAMP NOT NULL , `end_time` TIMESTAMP NOT NULL , `price_avg` FLOAT NOT NULL , `price_min` FLOAT NOT NULL , `price_max` FLOAT NOT NULL , `volume_avg` FLOAT NOT NULL , `volume_min` INT NOT NULL , `volume_max` INT NOT NULL , `cnt` INT NOT NULL ) ENGINE = InnoDB; 
```
Note: Order of columns matter, don't change them.

Open a new terminal and run `kafka-topics.sh --create --topic simple_data --bootstrap-server localhost:9092`, then run `kafka-topics.sh --create --topic agg_data --bootstrap-server localhost:9092` and `kafka-topics.sh --create --topic insert_data --bootstrap-server localhost:9092`

Now enter the directory containing the repo and run `python3 consumer.py simple_data`.

Open a new terminal and run `python3 consumer.py agg_data`.

Open a new terminal and run `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 sparkstream.py insert_data`. Again, the `spark-sql-kafka-0-10_2.12:3.1.2` refers to the scala version and kafka version. 

Open a new terminal and run `python3 producer.py insert_data`.

Open a new terminal and run `streamlit run lit.py`.

The rate of data flow can be changed in the last line of producer.py.

The Window size can be changed by changing the value in line 33 and line 77 of sparkstream.py , make sure withWatermark in line 32 is a few times higher than the window size.

The entire setup will take about 6 GB of RAM.

# Team
PES2UG20CS370: Tenzin Tsephel

PES2UG20CS371: Tushar Bhat

PES2UG20CS389: Vishwa Mehul Mehta

PES2UG20CS394: Voolla Sai Rethwik
