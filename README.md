# Data Catalog Service

Component responsible for populating the knowledge graph with metadata based on the aerOS Data Catalog Ontology.

## Docker

### Build

```bash
docker build -t data-catalog-service .
```

### Production Deployment

The container runs in production mode by default:

```bash
docker run --rm -it data-catalog-service
```

### Development Deployment

Run the Docker container as follows for development:

```bash
docker run --rm -it -v "$(pwd)"/data_catalog_service:/app/data_catalog_service:ro data-catalog-service dev
```

The command will mount the source code and start FastAPI in dev mode. The FastAPI server will listen on localhost and will reload upon changes on the source code.
