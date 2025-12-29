# DataFusion: Real-Time Data Integration Pipeline

A comprehensive rule-based data integration platform that enables users to build and execute scalable data processing pipelines through an intuitive user interface. The system supports both real-time streaming and batch processing with distributed computing capabilities.

## Features

### Core Capabilities

- **Rule-Based Processing**: User-friendly interface for defining data transformations without coding
- **Real-Time Streaming**: Apache Kafka integration for continuous data processing
- **Data Fusion**: Intelligent joining of datasets from multiple sources
- **Parallel Execution**: Multi-threaded pipeline execution with concurrent processing
- **Flexible Input Sources**: Support for Kaggle datasets, GitHub repositories, and Kafka streams
- **Multiple Output Destinations**: Local files, console logging, and RDBMS integration


### Technical Highlights

- **Distributed Computing**: Powered by Apache Spark for scalable data processing
- **Containerized Deployment**: Docker-based architecture for consistent environments
- **NoSQL Metadata Management**: MongoDB for storing rules, configurations, and pipeline metadata
- **Interactive Dashboard**: Streamlit-based user interface for configuration and monitoring


## Architecture

The system follows a modular microservices architecture with the following components:

```
DataFusion Pipeline Architecture
├── Data Ingestion Layer (Kafka + File Processing)
├── Processing Engine (PySpark + Rule Engine)
├── Data Fusion Layer (Multi-source joining)
├── Output Management (Local/RDBMS/Console)
└── User Interface (Streamlit Dashboard)
```


### Workflow

1. **Initialization**: Spark session creation and MongoDB connection
2. **Configuration**: Users define input sources, rules, fusion operations, and targets
3. **Pipeline Building**: Components are linked with hierarchical relationships
4. **Execution**: Parallel processing with persistent streaming support

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- At least 4GB RAM recommended
- Network access for external data sources


## Installation \& Setup

### Quick Start with Docker

1. **Clone the repository**
```bash
git clone https://github.com/Rohith-Kumar-S/datafusion-pipeline.git
cd datafusion-pipeline
```

2. **Launch the application**
```bash
docker-compose up -d
```

3. **Access the application**

- Open your browser and navigate to `http://localhost:8501`
- The Streamlit interface will load with configuration pages


## Project Structure

```
datafusion/
├── data/                          # Data storage directory
├── dependencies/                  # External dependencies and libraries
├── pages/                        # Streamlit application pages
│   ├── 1_data_ingestion.py       # Input source configuration
│   ├── 2_data_processor.py       # Rule engine interface
│   ├── 3_data_unifier.py         # Data fusion configuration
│   ├── 4_data_sinker.py          # Output destination setup
│   ├── 5_pipeline_builder.py     # Pipeline assembly interface
│   └── 6_pipeline_executer.py    # Pipeline execution and monitoring
├── app.py                        # Main Streamlit application
├── Dockerfile                    # Container configuration
├── pipeline_engine.py            # Core pipeline processing logic
├── pipeline_utils.py             # Utility functions and helpers
└── requirements.txt              # Python dependencies

docker-compose.yml                # Multi-container orchestration
README.md                        # Project documentation
```


## Usage

### Configure Input Sources

- Navigate to **Data Ingestion** page
- Add data sources (Kaggle links, GitHub repositories, or Kafka topics)
- Configure connection parameters and data schemas


### Define Processing Rules

- Go to **Data Processor** page
- Create transformation rules using available operations:
    - **Cast**: Data type conversion
    - **Rename**: Rename a column
    - **Filter**: Row-level data selection
    - **Drop**: Remove unnecessary columns, null values and duplicates
    - **Fill**: Remove columns, and null values
    - **Explode**: Handle nested data structures
    - **Flatten**: Normalize complex nested data
    - **Save**: Persist intermediate results


### Set Up Data Fusion

- Access **Data Merge** page
- Configure join operations between different data sources
- Define join keys and relationship types


### Configure Output Destinations

- Visit **Data Sinker** page
- Set up output targets:
    - Local file downloads
    - Console logging
    - RDBMS tables (via JDBC)


### Build and Execute Pipelines

- Use **Pipeline Builder** to assemble components
- Execute pipelines through **Pipeline Executer**
- Monitor real-time processing status and logs


## Data Processing Operations

The rule engine supports the following transformation operations:


| Operation | Description | Use Case |
| :-- | :-- | :-- |
| **Cast** | Convert data types | String to numeric conversion |
| **Rename** | Rename a column | Rename a column |
| **Filter** | Apply conditional filtering | Remove invalid records |
| **Drop** | Remove columns | Schema simplification |
| **Fill** | Fill columns |Fill null values |
| **Explode** | Expand nested structures | JSON/XML data processing |
| **Flatten** | Normalize nested data | API response processing |
| **Save** | Persist intermediate data | Checkpoint creation |

## Integration Examples

### Kafka Streaming Integration

```python
# Example Kafka topic configuration
topic_name = "iot-sensor-data"
kafka_servers = "localhost:9092"
```


### Database Output Configuration

```python
# RDBMS connection example
jdbc_url = "jdbc:postgresql://localhost:5432/dbtablename"
table_name = "processed_sensor_data"
```


## Pipeline Execution Modes

### Batch Processing

- Processes complete datasets at scheduled intervals
- Optimal for large historical data analysis
- Terminates upon completion

### Stream Processing

- Continuous real-time data processing
- Persistent execution (survives application restarts)
- Low-latency data transformation

## Testing with Sample Data

The project includes sample IoT datasets for testing:

- Customer data (CI00100-CI00150)
- Device information (D000-D050)
- Sensor readings with temperature, status, and timestamp data

## Technologies Used

- **Backend**: Apache Spark (PySpark), Python 3.8+
- **Streaming**: Apache Kafka
- **Database**: MongoDB
- **Frontend**: Streamlit
- **Containerization**: Docker, Docker Compose
- **Data Processing**: Pandas, NumPy

**DataFusion** - Democratizing data integration through intelligent automation and user-friendly interfaces.

