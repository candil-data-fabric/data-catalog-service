

```bash

curl --location 'http://localhost:80/dataProducts' \
--header 'Content-Type: application/json' \
--data '{
  "name": "Data Product 101",
  "description": "Contains data related to the inventory of desks",
  "owner": "urn:User:UserAAA",
  "keywords": [
    "network lab DEVNET",
    "Marousi"
  ],
  "glossary_terms": [
    "https://w3id.org/aerOS/building#Desk"
  ],
  "mappings": [
    "urn:ACME:TriplesMap:TM101"
  ]
}'

```
