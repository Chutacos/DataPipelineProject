
# Football Insights - Performance Analytics Pipeline

This project aims to create a robust data pipeline that extracts, transforms, and visualizes football performance data from the English Premier League (EPL) using the API-Football sports API. The primary goal is to provide valuable insights into team formations, performance statistics, and player lineups throughout the season.

## Table of Contents
- [Prequisites](#project-overview)
- [Project Overview](#project-overview)
- [Dataset](#dataset)
- [Pipeline Architecture](#pipeline-architecture)
- [Schema Architecture](#schema-architecture)
- [Output](#output)
- [System Considerations](#system-considerations)
- [Installation and Setup](#installation-and-setup)
- [Usage](#usage)
- [Contributors](#contributors)

---

## Prequisites
There are a few prequirements to this project before being able to replicate it. The first prequisite is to sign for AWS free-tier or paid if you wish to further expand on this project. Once signed up for AWS, you must setup a few services. The four services that need to be setup is AWS S3, AWS glue, AWS VPC, and AWS redshift. AWS redshift includes a credit when signing up for the first time, but be aware this credit can be consumed quickly. AWS s3 is straight forward on settinng up. With S3 we created a bucket where our data would be uploaded via a script in this repo. AWS glue is the service where the ETL magic happens. AWS glue is where you setup the pipeline to load data into redshift. AWS redshift is the serverless data warehosue where our data is dumped into. Here we can query the data or you connect to the server via other tools such as tableau or directly from a jupyter notebook. AWS VPC is bit of a nuance because this is where you setup your IP traffic routing rules for your data warehouse. The easiest way to setup VPC is to allow all traffic to come in and go out.
The other prerequisite is you have to sign up for rapidapi and subscribe to the football api or which ever api you desire. This will generate an API key and you can store this in a seperate config.py file you can store in a .gitignore folder. Some of these services will take time to setup and can be nuance but you have to be patient. I have attached some links below to resources

* https://youtu.be/M_pIzPGiF04?si=tGBwtUnkDS-MSQRG
* https://stackoverflow.com/questions/73254895/aws-glue-gives-accessdeniedexception
* https://stackoverflow.com/questions/63082626/getting-the-exception-thrown-in-awaitresult-error-when-trying-to-copy-table-i

---

## Project Overview
The **Football Insights - Performance Analytics Pipeline** is designed to fetch live football data using the [API-Football](https://rapidapi.com/api-sports/api/api-football) from RapidAPI. This data is then transformed and loaded into an AWS Redshift Data Warehouse, enabling efficient querying and visualization through a public Tableau dashboard.

- **Repository**: [GitHub Project Link](https://github.com/Chutacos/DataPipelineProject/)
- **Dashboard**: [Tableau Dashboard](https://public.tableau.com/shared/4Z4CRD87K?:display_count=n&:origin=viz_share_link)

---

## Dataset
The project utilizes the **API-Football sports API** from RapidAPI, fetching data related to:
- **Fixtures**: Match outcomes
- **Fixture-Statistics**: Team performance statistics
- **Fixture-Lineups**: Formations and player lineups

### Key Features:
- Live updates throughout the season
- Data is retrieved in JSON format and converted to a tabular structure
- Focuses on the current season of the English Premier League (EPL)
- API access is secured using private API keys

---

## Pipeline Architecture
The data pipeline is hosted on **AWS** and follows this workflow:

1. **Data Extraction**:
   - Python scripts automatically call API endpoints weekly to retrieve the latest football data in JSON format.
   - Data is converted to CSV and uploaded to an **AWS S3 bucket**.

2. **Data Transformation**:
   - **AWS Glue** is used as the ETL service to perform SQL transformations, reorganizing the data for optimal join logic and queries.

3. **Data Loading**:
   - Transformed data is loaded into an **AWS Redshift Data Warehouse** for persistent storage and querying.

4. **Data Visualization**:
   - A public **Tableau Dashboard** connects to the Redshift data store, visualizing key performance metrics.

---

## Schema Architecture
The pipeline's architecture includes the following components:
- **AWS S3**: Staging ground for raw CSV data
- **AWS Glue**: ETL service for data transformation
- **AWS Redshift**: Data Warehouse for persistent storage and efficient querying
- **Tableau**: Visualization tool for performance analytics

---

## Output
The **Tableau Dashboard** provides the following insights:
- Number of formations used by each team
- Team performance statistics across different metrics
- Player positional analysis using pie charts
- Home and away goal distribution by venue

[Tableau Dashboard](https://public.tableau.com/shared/4Z4CRD87K?:display_count=n&:origin=viz_share_link)

---

## System Considerations
### Security
- Secure storage and access via AWS with multi-factor authentication.
- API access secured using private API keys.

### Scalability
- **Horizontal and Vertical Scaling**: The system can scale to handle increased data volume by leveraging AWS's advanced account tiers.
- **Extensibility**: New features, data sources, and statistical insights can be integrated seamlessly.

### Future Enhancements
- Incorporate additional leagues and historical data views.
- Expand analytical features for more in-depth team and player performance analysis.

---

## Installation and Setup
1. **Clone the repository:**
   ```bash
   git clone https://github.com/Chutacos/DataPipelineProject.git
   cd DataPipelineProject
   ```

2. **Set up a virtual environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Configure API Keys and AWS Credentials:**
   - Update the `.env` file with your RapidAPI and AWS credentials.

---

## Usage
1. **Data Extraction:**
   - Run the Python script to fetch the latest football data.
     ```bash
     python s3_upload.py
     ```

2. **Data Transformation and Loading:**
   - Data is automatically processed using AWS Glue and loaded into Redshift.
   - Run the ETL scripts in this repo and update where necessary with your AWS information (s3 bucket and redshift)

3. **Data Visualization:**
   - Access the Tableau Dashboard to view updated performance metrics.

---

## Contributors
- **Jose Guarneros Padilla**
- **Akshat Patni**
- **Sahil Wadhwa**

---

## Acknowledgments
- **API-Football** for providing comprehensive football data.
- **AWS** for robust cloud infrastructure and data warehousing.
- **Tableau** for data visualization and insights.
