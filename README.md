
# Traffic Monitoring System

A cloud-based system for monitoring traffic using video analysis. The system processes traffic camera footage to detect vehicles, track their movement, calculate speeds, and generate real-time alerts for speeding violations.

## Architecture

This project uses Azure cloud services with a simplified architecture:

- **Azure Blob Storage**: Stores raw traffic videos and processed segments
- **Azure Functions**: Segments videos and processes real-time alerts
- **Azure Event Hub**: Handles event streaming for analysis and alerts
- **Azure Cosmos DB**: Stores vehicle detection and speed data
- **Container App**: Performs computer vision for vehicle detection and tracking

## Features

- Video preprocessing to split long videos into manageable segments
- Vehicle detection and tracking using computer vision
- Speed calculation based on 20m reference lines
- Storage of historical traffic data for reporting
- Real-time alerts for vehicles exceeding 130 km/h
- Statistical analysis of traffic patterns

## Getting Started

### Prerequisites

- Python 3.9 or higher
- Azure subscription
- Azure CLI
- Docker (for container deployment)

### Installation

1. Clone the repository

    git clone [https://github.com/yourusername/traffic-monitoring-system.git](https://github.com/yourusername/traffic-monitoring-system.git) 
    cd traffic-monitoring-system

2. Install dependencies

    pip install -r requirements.txt

3. Set up environment variables (see `.env.example`)

4. Set up Azure resources (Blob Storage, Cosmos DB, Event Hub)

### Running the application

1. Start the Azure Function app

    cd azure_functions 
    func start
    
2. Run test scripts

    python scripts/test_blob_trigger.py path/to/video.mp4

## Project Structure

- `src/`: Core source code
- `azure_functions/`: Azure Functions implementations
- `container_app/`: Container application for video processing
- `scripts/`: Utility scripts
- `tests/`: Test files

## Contributors

- [Chrysa Rizeakou](https://github.com/Chrysa712)
- [Erika Bajrami](https://github.com/empairami)
- [Ioannis Papadatos](https://github.com/Papajohn77)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
