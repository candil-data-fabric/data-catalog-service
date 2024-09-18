__name__ = "Data Catalog Service"
__version__ = "1.0.0"

import logging
import os
import sys

from fastapi import Body, FastAPI
from pydantic import BaseModel, Field
from rdflib import RDF, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCAT, DCTERMS, SKOS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# STARTUP ENV VARIABLES
CONTEXT_BROKER_URL = os.getenv("CONTEXT_BROKER_URL", "context-broker-1:8080")
DOMAIN_URI = os.getenv("DOMAIN_URI", "http://example.org/ACME/domain/default")
ORGANIZATION_URI = os.getenv("ORGANIZATION_URI", "http://example.org/ACME")

# RDF NAMESPACES
AERDCAT = Namespace("https://w3id.org/aerOS/data-catalog#")
AEROS = Namespace("https://w3id.org/aerOS/continuum#")

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
    business_glossary_terms: list[str] = Field(
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
g = Graph()
g.bind("aerdcat", AERDCAT)
g.bind("aeros", AEROS)

# Init domain node
domain = URIRef(DOMAIN_URI)

# Create Data Catalog
catalog = URIRef(DOMAIN_URI + "/data-catalog")
g.add((catalog, RDF.type, DCAT.Catalog))
g.add((catalog, AEROS.domain, domain))

# Create Business Glossary and link it to Data Catalog
glossary = URIRef(DOMAIN_URI + "/business-glossary")
g.add((glossary, RDF.type, SKOS.ConceptScheme))
g.add((catalog, DCAT.themeTaxonomy, glossary))

# Create Context Broker and link it to Data Catalog
cb = URIRef(DOMAIN_URI + "/context-broker")
g.add((cb, RDF.type, AERDCAT.ContextBroker))
g.add((catalog, AERDCAT.contextBroker, cb))
g.add((cb, AEROS.domain, domain))
g.add((cb, DCAT.endpointURL, URIRef(CONTEXT_BROKER_URL)))
g.add((
    cb,
    DCTERMS.conformsTo,
    URIRef("https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.08.01_60/gs_CIM009v010801p.pdf")
))

# Print graph in Turtle format
logger.debug(g.serialize(format='turtle'))

# Start FastAPI server:
app = FastAPI(
    title=__name__ + " - REST API",
    version=__version__
)

@app.post(
        path="/data-products",
        description="Registration of a data product in the data catalog.")
async def register_data_product(create_dp: CreateDataProduct = Body(...)):
    # Data Product
    dp = URIRef(DOMAIN_URI + "/dp/" + create_dp.name)
    g.add((dp, RDF.type, AERDCAT.DataProduct))
    g.add((dp, DCTERMS.identifier, Literal(create_dp.name))) # TODO: Change to dcterms:identifier
    g.add((dp, DCTERMS.description, Literal(create_dp.description)))
    # Owner
    owner = URIRef(create_dp.owner)
    g.add((dp, DCTERMS.publisher, owner)) # TODO: Fix ontology dcterms:publisher
    # Keywords
    if create_dp.keywords:
        for keyword in create_dp.keywords:
            g.add((dp, DCAT.keyword, Literal(keyword)))
    # Business glossary terms
    for term_uri in create_dp.business_glossary_terms:
        term = URIRef(term_uri)
        g.add((term, RDF.type, SKOS.Concept))
        g.add((term, SKOS.inScheme, glossary))
        g.add((dp, DCAT.theme, term))
    # Distribution
    distribution = URIRef(DOMAIN_URI + "/distribution/" + create_dp.name)
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
    logger.debug(g.serialize(format='turtle'))
    # TODO: Serialize to NGSI-LD and send to Context Broker
    return dp

## -- END MAIN CODE -- ##
