__name__ = "Data Catalog Service"
__version__ = "1.0.0"

import logging
import os
import socket
import sys
import urllib.parse
from uuid import uuid4

from confluent_kafka import KafkaException, Producer
from fastapi import Body, FastAPI
from pydantic import BaseModel, Field
from rdflib import RDF, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCAT, DCTERMS, SKOS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler(sys.stdout)
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)

# STARTUP ENV VARIABLES
CONTEXT_BROKER_URL = os.getenv("CONTEXT_BROKER_URL", "context-broker-1:8080")
DOMAIN_URI = os.getenv("DOMAIN_URI", "urn:ACME:Domain:default")
ORGANIZATION_URI = os.getenv("ORGANIZATION_URI", "urn:ACME")

# RDF TO NGSI-LD Translation
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "knowledge-graphs")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", None)

# RDF NAMESPACES
AERDCAT = Namespace("https://w3id.org/aerOS/data-catalog#")
AEROS = Namespace("https://w3id.org/aerOS/continuum#")

# Kafka Producer Callback
def delivery_report(errmsg, msg):
    """
    Reports the Failure or Success of a message delivery.
    Args:
        errmsg (KafkaError): The Error that occurred while message producing.
        msg (Actual message): The message that was produced.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if errmsg is not None:
        logger.error("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
        return
    logger.debug('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

# Connect to Kafka
conf = {'bootstrap.servers': KAFKA_BROKER,
        'client.id': socket.gethostname()}
logger.info("Subscribing to Kafka topic {0}...".format(KAFKA_TOPIC))
try:
    kafka_producer = Producer(conf)
except KafkaException as e:
    logger.warning(f'Exception:{e}')

class CreateDataProduct(BaseModel):
    name: str = Field(
        description="Name of the data product. "
                    "This will uniquely identify the data product in the data catalog."
    )
    description: str = Field(
        description="Description of the data product."
    )
    owner: str = Field(
        description="URI that identifies the owner of the data product."
    )
    keywords: list[str] = Field(
        default=None,
        description="(Optional) List of custom keywords/tags that identify the data product."
    )
    glossary_terms: list[str] = Field(
        description="List of URIs identiftying concepts associated with the data product. "
                    "These concepts must be captured in existing ontologies."
    )
    mappings: list[str] = Field(
        default=None,
        description="(Optional) List of URIs that identify the RML mappings used for the creation of"
                    "the data product, i.e., TripleMapping. "
                    "Only applies to data products where raw data have been transformed into RDF."
    )

## -- BEGIN MAIN CODE -- ##

# Init graph
g_init = Graph()
g_init.bind("aerdcat", AERDCAT)
g_init.bind("aeros", AEROS)

# Init domain node
domain = URIRef(DOMAIN_URI)

# Create Data Catalog
catalog = URIRef(DOMAIN_URI + ":" + "DataCatalog")
g_init.add((catalog, RDF.type, DCAT.Catalog))
g_init.add((catalog, AEROS.domain, domain))

# Create Business Glossary and link it to Data Catalog
glossary = URIRef(DOMAIN_URI + ":"+ "BusinessGlossary")
g_init.add((glossary, RDF.type, SKOS.ConceptScheme))
g_init.add((catalog, DCAT.themeTaxonomy, glossary))

# Create Context Broker and link it to Data Catalog
cb = URIRef(DOMAIN_URI + ":" + "ContextBroker")
g_init.add((cb, RDF.type, AERDCAT.ContextBroker))
g_init.add((catalog, AERDCAT.contextBroker, cb))
g_init.add((cb, AEROS.domain, domain))
g_init.add((cb, DCAT.endpointURL, URIRef(CONTEXT_BROKER_URL)))
g_init.add((
    cb,
    DCTERMS.conformsTo,
    URIRef("https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.08.01_60/gs_CIM009v010801p.pdf")
))
logger.info(g_init.serialize(format='turtle'))
kafka_producer.produce(
    topic=KAFKA_TOPIC, key=str(uuid4()),
    value=g_init.serialize(format='turtle'),
    on_delivery=delivery_report
)
kafka_producer.poll(1)

# Start FastAPI server:
app = FastAPI(
    title=__name__ + " - REST API",
    version=__version__
)

@app.post(
        path="/dataProducts",
        description="Registration of a data product in the data catalog.")
async def register_data_product(create_dp: CreateDataProduct = Body(...)):
    # Init graph
    g = Graph()
    g.bind("aerdcat", AERDCAT)
    g.bind("aeros", AEROS)
    # Data Product
    dp = URIRef(DOMAIN_URI + ":" + "DataProduct" + ":" + urllib.parse.quote(create_dp.name))
    g.add((dp, RDF.type, AERDCAT.DataProduct))
    g.add((dp, DCTERMS.identifier, Literal(create_dp.name)))
    g.add((dp, DCTERMS.description, Literal(create_dp.description)))
    # Owner
    owner = URIRef(create_dp.owner)
    g.add((dp, DCTERMS.publisher, owner))
    # Keywords
    if create_dp.keywords:
        for keyword in create_dp.keywords:
            g.add((dp, DCAT.keyword, Literal(keyword)))
    # Business glossary terms
    for term_uri in create_dp.glossary_terms:
        term = URIRef(term_uri)
        g.add((term, RDF.type, SKOS.Concept))
        g.add((term, SKOS.inScheme, glossary))
        g.add((dp, DCAT.theme, term))
    # Distribution
    distribution = URIRef(DOMAIN_URI + ":" + "Distribution" + ":" + urllib.parse.quote(create_dp.name))
    g.add((distribution, RDF.type, DCAT.Distribution))
    g.add((distribution, DCAT.accessURL, URIRef(CONTEXT_BROKER_URL)))
    g.add((
        distribution,
        DCAT.mediaType,
        URIRef("http://www.iana.org/assignments/media-types/application/ld+json")
    ))
    # Link data product to distribution
    g.add((dp, DCAT.distribution, distribution))
    # Link distribution to context broker
    g.add((distribution, DCAT.accessService, cb))
    # Link data service to data product
    g.add((cb, AERDCAT.servesDataProduct, dp))
    # Mappings
    if create_dp.mappings:
        for mapping_uri in create_dp.mappings:
            mapping = URIRef(mapping_uri)
            g.add((dp, AERDCAT.mapping, mapping))
    # Link DP to Data Catalog
    g.add((catalog, AERDCAT.dataProduct, dp))
    # Sending RDF data to Kafka for NGSI-LD translation
    kafka_producer.produce(
        topic=KAFKA_TOPIC, key=str(uuid4()),
        value=g.serialize(format='turtle'),
        on_delivery=delivery_report
    )
    kafka_producer.poll(1)
    return dp

## -- END MAIN CODE -- ##
