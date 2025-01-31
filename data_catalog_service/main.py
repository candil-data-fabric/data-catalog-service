__name__ = "Data Catalog Service"
__version__ = "1.1.2"

import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Body, FastAPI, status
from pydantic import BaseModel, Field
from rdf_to_ngsi_ld.translator import send_to_context_broker, serializer
from rdflib import RDF, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCAT, DCTERMS, SKOS

# STARTUP ENV VARIABLES
INTERNAL_CONTEXT_BROKER_URL = os.getenv(
    "INTERNAL_CONTEXT_BROKER_URL",
    "http://localhost:1026/ngsi-ld/v1")
PUBLIC_CONTEXT_BROKER_URL = os.getenv(
    "PUBLIC_CONTEXT_BROKER_URL",
    "http://orion-ld:1026/ngsi-ld/v1")
ORGANIZATION_ID = os.getenv("ORGANIZATION_ID", "NCSRD")
ORGANIZATION_URI = "urn:Organization:" + ORGANIZATION_ID
DOMAIN_ID = os.getenv("DOMAIN_ID", "Default")
DOMAIN_URI = "urn:Organization:" + ORGANIZATION_ID + ":Domain:" + DOMAIN_ID

# RDF NAMESPACES
AERDCAT = Namespace("https://w3id.org/aerOS/data-catalog#")
AEROS = Namespace("https://w3id.org/aerOS/continuum#")

# Logging
LOGLEVEL = os.getenv('LOGLEVEL', 'INFO').upper()

logger = logging.getLogger(__name__)
logger.setLevel(LOGLEVEL)
stream_handler = logging.StreamHandler(sys.stdout)
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)

class CreateDataProduct(BaseModel):
    name: str = Field(
        description="Name of the data product. "
                    "This will uniquely identify the data product in the data catalog."
    )
    description: str = Field(
        description="Description of the data product."
    )
    owner: str = Field(
        description="aerOS username that identifies the owner of the data product."
    )
    keywords: Optional[list[str]] = Field(
        default=None,
        description="(Optional) List of custom keywords/tags that identify the data product."
    )
    glossary_terms: list[str] = Field(
        description="List of URIs identiftying concepts associated with the data product. "
                    "These concepts must be captured in existing ontologies."
    )

## -- BEGIN MAIN CODE -- ##

# Init graph
core_graph = Graph()
core_graph.bind("aerdcat", AERDCAT)
core_graph.bind("aeros", AEROS)

# Init global subjects
domain = URIRef(DOMAIN_URI)
catalog = URIRef(DOMAIN_URI + ":" + "Catalog")
cb = URIRef(DOMAIN_URI + ":" + "ContextBroker")
glossary = URIRef(DOMAIN_URI + ":"+ "BusinessGlossary")
organization = URIRef(ORGANIZATION_URI)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global core_graph
    # Create catalog at the domain level
    core_graph.add((catalog, RDF.type, DCAT.Catalog))
    core_graph.add((catalog, AEROS.domain, domain))

    # Create Business Glossary and link it to catalog
    core_graph.add((glossary, RDF.type, SKOS.ConceptScheme))
    core_graph.add((catalog, DCAT.themeTaxonomy, glossary))

    # Create Context Broker and link it to catalog
    core_graph.add((cb, RDF.type, AERDCAT.ContextBroker))
    core_graph.add((catalog, AERDCAT.contextBroker, cb))
    core_graph.add((cb, AEROS.domain, domain))
    core_graph.add((cb, DCAT.endpointURL, URIRef(PUBLIC_CONTEXT_BROKER_URL)))
    core_graph.add((
        cb,
        DCTERMS.conformsTo,
        URIRef("https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.06.01_60/gs_CIM009v010601p.pdf")
    ))
    # Sending RDF data to serializer for NGSI-LD translation
    entities = serializer(core_graph)
    debug = False
    if LOGLEVEL == "DEBUG":
        debug = True
    send_to_context_broker(entities, INTERNAL_CONTEXT_BROKER_URL, debug)

    yield

# Start FastAPI server:
app = FastAPI(
    title=__name__ + " - REST API",
    version=__version__,
    lifespan=lifespan
)

@app.post(
        path="/dataProducts",
        description="Registration of a data product in the data catalog.")
async def register_data_product(create_dp: CreateDataProduct = Body(...)) -> URIRef:
    global core_graph
    # Init graph
    g = Graph()
    g.bind("aerdcat", AERDCAT)
    g.bind("aeros", AEROS)
    # Data Product
    dp_id = create_dp.name.replace(" ","_")
    dp = URIRef(DOMAIN_URI + ":" + "DataProduct" + ":" + dp_id)
    g.add((dp, RDF.type, AERDCAT.DataProduct))
    g.add((dp, DCTERMS.identifier, Literal(dp_id)))
    g.add((dp, DCTERMS.description, Literal(create_dp.description)))
    # Ownership
    owner_user = URIRef("urn:User:" + create_dp.owner)
    g.add((dp, DCTERMS.publisher, owner_user))
    g.add((dp, DCTERMS.publisher, organization))
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
    distribution = URIRef(
        DOMAIN_URI + ":" + "DataProduct" + ":" + dp_id + ":" + "Distribution")
    g.add((distribution, RDF.type, DCAT.Distribution))
    g.add((distribution, DCAT.accessURL, URIRef(PUBLIC_CONTEXT_BROKER_URL)))
    g.add((
        distribution,
        DCAT.mediaType,
        URIRef("http://www.iana.org/assignments/media-types/application/ld+json")
    ))
    # Link data product to distribution
    g.add((dp, DCAT.distribution, distribution))
    # Link distribution to context broker (data service)
    g.add((distribution, DCAT.accessService, cb))
    # Add domain's context broker (data service) and link to the new data product
    for s, p, o in core_graph.triples((cb, AERDCAT.servesDataProduct, None)):
        g.add((s, p, o))
    g.add((cb, AERDCAT.servesDataProduct, dp))
    g.add((cb, RDF.type, AERDCAT.ContextBroker))
    # Add domain's catalog and register data product
    for s, p, o in core_graph.triples((catalog, AERDCAT.dataProduct, None)):
        g.add((s, p, o))
    g.add((catalog, AERDCAT.dataProduct, dp))
    g.add((catalog, RDF.type, DCAT.Catalog))
    # Store request graph in core_graph
    core_graph = core_graph + g
    # Sending RDF data to serializer for NGSI-LD translation
    entities = serializer(g)
    debug = False
    if LOGLEVEL == "DEBUG":
        debug = True
    send_to_context_broker(entities, INTERNAL_CONTEXT_BROKER_URL, debug)
    return dp

## -- END MAIN CODE -- ##
