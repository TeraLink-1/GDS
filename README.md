# Ground Data System (GDS)

A microservices-based ground data system for processing telemetry, commands, and events from flight software. The system uses Redis Streams for message queuing and PostgreSQL for historical data storage.

## Components

**Core Services:**
- **fprime_link**: TCP/IP server that receives F Prime protocol frames from flight software and provides WebSocket interface for bidirectional communication
- **decomm_service**: Processes raw frames from Redis streams and extracts telemetry points
- **archiver**: Persists telemetry data to PostgreSQL for historical analysis
- **telemetry_gateway**: REST API and WebSocket server for accessing historical and real-time telemetry
- **gs_control**: Mock ground station control service for command acknowledgment

**Infrastructure:**
- **Redis**: Message queue using Redis Streams
- **PostgreSQL**: Historical telemetry storage
- **OpenMCT**: Web-based mission control interface

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)

### Running the System

Start all services with Docker Compose:

```bash
docker-compose up --build
```

This will start:
- Redis on port 6379
- PostgreSQL on port 5432
- Telemetry Gateway API on port 8080
- OpenMCT on port 8081
- F Prime Link TCP server on port 50000
- F Prime Link WebSocket on port 50001

### Environment Variables

Key environment variables (with defaults):
- `REDIS_URL`: Redis connection string (default: `redis://localhost:6379/0`)
- `DB_URL`: PostgreSQL connection string
- `SAT_ID`: Satellite identifier (default: `TERALINK-1`)
- `GS_ID`: Ground station identifier (default: `GS-DEMO`)
- `FPRIME_TCP_PORT`: F Prime TCP server port (default: `50000`)
- `FPRIME_WS_PORT`: F Prime WebSocket port (default: `50001`)

### Testing

The `raw_frame_producer` example service generates mock telemetry data for testing the pipeline. It's included in the docker-compose setup.

## Development

Install dependencies:

```bash
pip install -r requirements.txt
```

Run individual services locally by setting the appropriate environment variables and executing the service's `main.py` file.
