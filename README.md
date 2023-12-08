# Big-dataBig
Data Project

The Big Data Project is a comprehensive endeavor showcasing the integration of Apache Airflow, a robust workflow management platform, with various Big Data technologies to construct a holistic data pipeline.
The primary objective is to extract data from external sources, format and prepare it, leverage Apache Spark for data analysis, and ultimately derive meaningful insights.

Data Sources and API:
The project utilizes the Free Foreign Exchange, Crypto Rates & EU VAT Rates API (Exchangerate) for exchange rate data and the News API (MediaStack) for daily news.
The Exchangerate API provides currency exchange rates with the euro as the base, while the MediaStack API supplies daily news with author, title, description, and date.

Structure of the Data Lake:
The data lake is organized into distinct directories for raw data, formatted data, and analyzed data. Raw data is obtained from the APIs, formatted data undergoes processing and structuring, and analyzed data is used for generating insights. 
The structure includes subdirectories for each day's data, facilitating historical tracking.

Data Pipeline:
The data pipeline involves fetching raw data from the Exchangerates and MediaStack APIs, converting it into formatted data, combining datasets, analyzing data using Apache Spark, and indexing the results into Elasticsearch for efficient querying. 
The pipeline is orchestrated through Apache Airflow, using Directed Acyclic Graphs (DAGs) to manage tasks.

DAGs Episodes:
The project is segmented into episodes, covering fundamental Airflow concepts, hands-on exercises, and the integration with Git for version control. 
Subsequent episodes delve into data extraction from APIs, data formatting, combination, and analysis.

Usage and Visualization:
The project includes directories for usage and visualization, where the analyzed and indexed data is stored. 
This allows for further exploration and utilization of the insights generated from the data pipeline.
