__name__ = "Data Catalog Service"
__version__ = "1.4.0"
__author__ = [
    "Ignacio Domínguez Martínez-Casanueva",
    "David Martínez García"
]
__credits__ = [
    "Telefónica I+D",
    "GIROS DIT-UPM",
    "Ignacio Domínguez Martínez-Casanueva",
    "Luis Bellido Triana",
    "Lucía Cabanillas Rodríguez",
    "David Martínez García"
]

## -- BEGIN IMPORT STATEMENTS -- ##

from contextlib import asynccontextmanager
from fastapi import Body, FastAPI, HTTPException
import json
import logging
import os
from pydantic import BaseModel, Field
import pymongo
from rdf_to_ngsi_ld.translator import send_to_context_broker, serializer
from rdflib import RDF, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCAT, DCTERMS, SKOS
import requests
import sys
from typing import Optional

## -- END IMPORT STATEMENTS -- ##

## -- BEGIN CONSTANTS DECLARATION -- ##

### LOGGING ###

LOGLEVEL = os.getenv('LOGLEVEL', 'INFO').upper()

### --- ###

### CONTEXT BROKER ###

INTERNAL_CONTEXT_BROKER_URL = os.getenv(
    "INTERNAL_CONTEXT_BROKER_URL",
    "http://localhost:1026/ngsi-ld/v1"
)
PUBLIC_CONTEXT_BROKER_URL = os.getenv(
    "PUBLIC_CONTEXT_BROKER_URL",
    "http://orion-ld:1026/ngsi-ld/v1"
)

### --- ###

### ORGANIZATION AND DOMAIN ###

ORGANIZATION_ID = os.getenv("ORGANIZATION_ID", "NCSRD")
ORGANIZATION_URI = "urn:ngsi-ld:Organization:" + ORGANIZATION_ID
DOMAIN_ID = os.getenv("DOMAIN_ID", "Default")
DOMAIN_URI = "urn:ngsi-ld:Domain:" + DOMAIN_ID

### --- ###

### RDF NAMESPACES ###

AERDCAT = Namespace("https://w3id.org/aerOS/data-catalog#")
AEROS = Namespace("https://w3id.org/aerOS/continuum#")

### --- ###

### MONGO-DB INFORMATION ###

MONGO_DB_URI = os.getenv("MONGO_DB_URI")
CORE_GRAPH_ID = "core-graph"

### --- ###

## -- END CONSTANTS DECLARATION -- ##

## -- BEGIN LOGGING CONFIGURATION -- ##

logger = logging.getLogger(__name__)
logger.setLevel(LOGLEVEL)
stream_handler = logging.StreamHandler(sys.stdout)
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)

## -- END LOGGING CONFIGURATION -- ##

## -- BEGIN DEFINITION OF AUXILIARY FUNCTIONS -- ##

def subject_has_no_relationship(graph: Graph, subject, predicate) -> bool:
    '''
    Checks if a subject does not have a predicate.
    '''

    return next(graph.triples((subject, predicate, None)), None) is None

def check_if_graph_exists_in_mongodb(graph_id: str) -> bool:
    '''
    Checks if the RDF graph which ID is passed as a parameter exists in MongoDB.
    The document saved in MongoDB is a dictionary that consists of the following key/values:
    - "_id": ID of the RDF graph (string).
    - "@graph": JSON-LD representation of the RDF graph (dictionary).
    '''

    logger.info("Checking if the RDF graph exists in MongoDB...")

    count = mongodb_collection.count_documents({"_id": graph_id})
    if count == 0:
        return False
    else:
        return True

def get_graph_from_mongodb(graph_id: str) -> Graph:
    '''
    Returns the RDF graph object which JSON-LD representation is stored in
    MongoDB and which ID is passed as a parameter.
    The document saved in MongoDB is a dictionary that consists of the following key/values:
    - "_id": ID of the RDF graph (string).
    - "@graph": JSON-LD representation of the RDF graph (dictionary).
    '''

    logger.info("Retrieving the RDF graph from MongoDB...")

    dict = list(mongodb_collection.find({"_id": graph_id}))[0]
    graph = Graph().parse(data = json.dumps(dict["@graph"]), format = "json-ld")
    return graph

def write_graph_to_mongodb(graph_id: str, graph: Graph) -> None:
    '''
    Serializes the RDF graph object to JSON-LD format and saves it into MongoDB.
    The document saved in MongoDB is a dictionary that consists of the following key/values:
    - "_id": ID of the RDF graph (string).
    - "@graph": JSON-LD representation of the RDF graph (dictionary).
    '''

    logger.info("Serializing the RDF graph to JSON-LD format and saving it in MongoDB...")

    json_ld = graph.serialize(format = "json-ld")
    dict = {}
    dict["_id"] = graph_id
    dict["@graph"] = json.loads(json_ld)
    mongodb_collection.replace_one({"_id": graph_id}, dict, True) # Using upsert operation.

## -- END DEFINITION OF AUXILIARY FUNCTIONS -- ##

## -- BEGIN DEFINITION OF PYDANTIC MODELS -- ##

class CreateDataProduct(BaseModel):
    id: str = Field(
        description = "Unique identifer of the data product in the Data Catalog."
    )
    name: str = Field(
        description = "Name of the data product."
    )
    description: str = Field(
        description = "Description of the data product."
    )
    owner: str = Field(
        description = "aerOS username that identifies the owner of the data product."
    )
    keywords: Optional[list[str]] = Field(
        default = None,
        description = "(Optional) List of custom keywords/tags that identify the data product."
    )
    glossary_terms: list[str] = Field(
        description = "List of URIs identifying concepts associated with the data product. These concepts must be captured in existing ontologies."
    )

## -- END DEFINITION OF PYDANTIC MODELS -- ##

## -- BEGIN MAIN CODE -- ##

# Initialize MongoDB client:
mongodb_client = pymongo.MongoClient(MONGO_DB_URI)
mongodb_database = mongodb_client["data-fabric-data-catalog-service"]
mongodb_collection = mongodb_database["graphs"]

# Init global subjects:
domain = URIRef(DOMAIN_URI)
catalog = URIRef(
    ORGANIZATION_URI + ":Domain:" +
    DOMAIN_ID + ":Catalog"
)
cb = URIRef(
    ORGANIZATION_URI + ":Domain:" +
    DOMAIN_ID + ":ContextBroker"
)
glossary = URIRef(
    ORGANIZATION_URI + ":Domain:" +
    DOMAIN_ID + ":BusinessGlossary"
)
organization = URIRef(ORGANIZATION_URI)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global core_graph

    # -> CODE TO BE EXECUTED UPON STARTUP
    
    # Check if the RDF graph exists in MongoDB.
    # If it does not exist, it is created and saved in memory and in MongoDB.
    # Otherwise, it is retrieved from MongoDB and saved in memory.
    if check_if_graph_exists_in_mongodb(graph_id = CORE_GRAPH_ID) == False:
        logger.info("The RDF graph does not exist in MongoDB. Creating it...")

        # Create RDF graph (core_graph) and bind it to namespaces:
        core_graph = Graph()
        core_graph.bind("aerdcat", AERDCAT)
        core_graph.bind("aeros", AEROS)

        # Create Catalog at the Domain level:
        core_graph.add((catalog, RDF.type, DCAT.Catalog))
        core_graph.add((catalog, AEROS.domain, domain))

        # Create BusinessGlossary and link it to Catalog:
        core_graph.add((glossary, RDF.type, SKOS.ConceptScheme))
        core_graph.add((catalog, DCAT.themeTaxonomy, glossary))

        # Create ContextBroker and link it to Catalog:
        core_graph.add((cb, RDF.type, AERDCAT.ContextBroker))
        core_graph.add((catalog, AERDCAT.contextBroker, cb))
        core_graph.add((cb, AEROS.domain, domain))
        core_graph.add((cb, DCAT.endpointURL, URIRef(PUBLIC_CONTEXT_BROKER_URL)))
        core_graph.add((
            cb,
            DCTERMS.conformsTo,
            URIRef("https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.06.01_60/gs_CIM009v010601p.pdf")
        ))

        logger.info("RDF graph created.")

        # Send RDF data to serializer for NGSI-LD translation:
        entities = serializer(core_graph)
        debug = False
        if LOGLEVEL == "DEBUG":
            debug = True
        send_to_context_broker(entities, INTERNAL_CONTEXT_BROKER_URL, debug)

        # The RDF graph is saved in MongoDB:
        write_graph_to_mongodb(graph_id = CORE_GRAPH_ID, graph = core_graph)
        logger.info("RDF graph saved in MongoDB.")
    else:
        # The RDF graph is retrieved from MongoDB and saved in memory (core_graph):
        logger.info("The RDF graph exists in MongoDB.")
        core_graph = get_graph_from_mongodb(graph_id = CORE_GRAPH_ID)
        logger.info("RDF graph retrieved from MongoDB.")

    # <- CODE TO BE EXECUTED UPON STARTUP

    yield

    # -> CODE TO BE EXECUTED UPON SHUTDOWN

    # Upon shutdown, the RDF graph (core_graph) is always saved in MongoDB.
    write_graph_to_mongodb(graph_id = CORE_GRAPH_ID, graph = core_graph)
    logger.info("RDF graph saved in MongoDB.")

    # <- CODE TO BE EXECUTED UPON SHUTDOWN

# Start FastAPI server:
app = FastAPI(
    title = __name__ + " - REST API",
    version = __version__,
    lifespan = lifespan
)

@app.post(
    path = "/dataProducts",
    description = "Registration of a data product in the Data Catalog."
)
async def register_data_product(create_dp: CreateDataProduct = Body(...)) -> str:
    global core_graph

    # Init request graph:
    g = Graph()
    g.bind("aerdcat", AERDCAT)
    g.bind("aeros", AEROS)

    # DataProduct:
    dp_id = create_dp.id
    dp = URIRef(
        ORGANIZATION_URI + ":Domain:" +
        DOMAIN_ID + ":DataProduct:" + dp_id
    )
    response = requests.get(
        INTERNAL_CONTEXT_BROKER_URL + "/entities/" + dp
    )
    if response.status_code != 404:
        raise HTTPException(
            status_code = 409, detail = "Data product already exists."
        )
    g.add((dp, RDF.type, AERDCAT.DataProduct))
    g.add((dp, DCTERMS.identifier, Literal(create_dp.id)))
    g.add((dp, DCTERMS.title, Literal(create_dp.name)))
    g.add((dp, DCTERMS.description, Literal(create_dp.description)))

    # Ownership:
    owner_user = URIRef("urn:User:" + create_dp.owner)
    g.add((dp, DCTERMS.publisher, owner_user))
    g.add((dp, DCTERMS.publisher, organization))

    # Keywords:
    if create_dp.keywords:
        for keyword in create_dp.keywords:
            g.add((dp, DCAT.keyword, Literal(keyword)))

    # Business Glossary terms:
    for term_uri in create_dp.glossary_terms:
        term = URIRef(term_uri)
        g.add((term, RDF.type, SKOS.Concept))
        g.add((term, SKOS.inScheme, glossary))
        g.add((dp, DCAT.theme, term))

    # Distribution:
    distribution = URIRef(
        ORGANIZATION_URI + ":Domain:" +
        DOMAIN_ID + ":DataProduct:" + dp_id +
        ":" + "Distribution"
    )
    g.add((distribution, RDF.type, DCAT.Distribution))
    g.add((distribution, DCAT.accessURL, URIRef(PUBLIC_CONTEXT_BROKER_URL)))
    g.add((
        distribution,
        DCAT.mediaType,
        URIRef("http://www.iana.org/assignments/media-types/application/ld+json")
    ))

    # Link DataProduct to Distribution:
    g.add((dp, DCAT.distribution, distribution))

    # Link Distribution to ContextBroker (data service):
    g.add((distribution, DCAT.accessService, cb))

    # Add Domain's ContextBroker (data service) and link to the new DataProduct:
    for s, p, o in core_graph.triples((cb, AERDCAT.servesDataProduct, None)):
        g.add((s, p, o))
    g.add((cb, AERDCAT.servesDataProduct, dp))
    g.add((cb, RDF.type, AERDCAT.ContextBroker))

    # Add Domain's Catalog and register DataProduct:
    for s, p, o in core_graph.triples((catalog, AERDCAT.dataProduct, None)):
        g.add((s, p, o))
    g.add((catalog, AERDCAT.dataProduct, dp))
    g.add((catalog, RDF.type, DCAT.Catalog))

    # Store request graph in core_graph:
    core_graph = core_graph + g

    # Send RDF data to serializer for NGSI-LD translation:
    entities = serializer(g,
        [str(AERDCAT.servesDataProduct),
        str(DCAT.keyword), str(AERDCAT.dataProduct), str(DCAT.theme)]
    )
    debug = False
    if LOGLEVEL == "DEBUG":
        debug = True
    send_to_context_broker(entities, INTERNAL_CONTEXT_BROKER_URL, debug)

    # The RDF graph (core_graph) is saved in MongoDB:
    write_graph_to_mongodb(graph_id = CORE_GRAPH_ID, graph = core_graph)
    logger.info("RDF graph saved in MongoDB.")
    
    return dp_id

@app.delete(
    path = "/dataProducts/{dp_id}",
    description = "Deletion of a data product in the Data Catalog."
)
async def delete_data_product(dp_id: str):
    global core_graph

    # Create a copy of core_graph for the request:
    g = core_graph

    # Delete DataProduct from graph and in NGSI-LD:
    dp = URIRef(
        ORGANIZATION_URI + ":Domain:" +
        DOMAIN_ID + ":DataProduct:" + dp_id
    )
    response = requests.get(
        INTERNAL_CONTEXT_BROKER_URL + "/entities/" + dp
    )
    if response.status_code != 200:
        raise HTTPException(
            status_code = 404, detail = "Data product not found."
        )
    g.remove((dp, None, None))
    response = requests.delete(
        INTERNAL_CONTEXT_BROKER_URL + "/entities/" + dp
    )

    # Delete Distribution from graph and in NGSI-LD:
    distribution = URIRef(
        ORGANIZATION_URI + ":Domain:" +
        DOMAIN_ID + ":DataProduct:" + dp_id +
        ":" + "Distribution"
    )
    g.remove((distribution, None, None))
    response = requests.delete(
        INTERNAL_CONTEXT_BROKER_URL + "/entities/" + distribution
    )
    
    # "servesDataProduct" and "dataProduct" relationships are deleted
    # in the in-memory RDF graph, and NGSI-LD entities are updated:
    g.remove((cb, AERDCAT.servesDataProduct, dp))
    g.remove((catalog, AERDCAT.dataProduct, dp))

    # Store request graph in core_graph:
    core_graph = g

    # Separate graph for update (fix to delete relationships):
    local_graph = Graph()
    local_graph = local_graph + g
    entities = []
    if subject_has_no_relationship(g, cb, AERDCAT.servesDataProduct):
        local_graph.add((cb, AERDCAT.servesDataProduct, URIRef("urn:ngsi-ld:null")))
        local_graph.add((catalog, AERDCAT.dataProduct, URIRef("urn:ngsi-ld:null")))
        # Send RDF data to serializer for NGSI-LD translation:
        entities = serializer(
            local_graph,
            [
                str(DCAT.keyword),
                str(DCAT.theme)
            ]
        )
    else:
        # Send RDF data to serializer for NGSI-LD translation:
        entities = serializer(
            local_graph,
            [
                str(AERDCAT.servesDataProduct),
                str(DCAT.keyword),
                str(AERDCAT.dataProduct),
                str(DCAT.theme)
            ]
        )
    debug = False
    if LOGLEVEL == "DEBUG":
        debug = True
    send_to_context_broker(entities, INTERNAL_CONTEXT_BROKER_URL, debug)

    # The RDF graph (core_graph) is saved in MongoDB:
    write_graph_to_mongodb(graph_id = CORE_GRAPH_ID, graph = core_graph)
    logger.info("RDF graph saved in MongoDB.")

## -- END MAIN CODE -- ##
