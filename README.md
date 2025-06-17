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

Or use the DEV docker-compose file to test the service a development scenario:
```bash
docker compose -f docker-compose-dev.yaml up -d
```


Sample request:

```bash
curl --location 'http://localhost:8000/dataProducts' \
--header 'Content-Type: application/json' \
--data '{
  "id": "864735B5-BD03-47E3-9C73-BD2A76F7634B",
  "name": "Data Product 101",
  "description": "Contains data related to the inventory of desks",
  "owner": "urn:User:UserAAA",
  "keywords": [
    "Marousi"
  ],
  "glossary_terms": [
    "https://w3id.org/aerOS/building#Desk"
  ]
}'
```

## Acknowledgements

This work was partially supported by the following projects:

- **UNICO 5G I+D 6G-DATADRIVEN**: Redes de próxima generación (B5G y 6G) impulsadas por datos para la fabricación sostenible y la respuesta a emergencias. Ministerio de Asuntos Económicos y Transformación Digital. European Union NextGenerationEU.

![UNICO](./images/ack-logo.png)
