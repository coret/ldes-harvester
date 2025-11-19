# Linked Data Event Streams Harvester

A dockerized Python script for harvesting Linked Data Event Streams (LDES) endpoints and caching RDF data as N-Triples files. 

## Features

- **Full LDES harvesting**: Automatically follows pagination and harvests all members from an LDES endpoint
- **N-Triples output**: Converts JSON-LD members to N-Triples format for easy processing (eg. import into triplestore)
- **Resume capability**: Automatically resumes from where it left off if interrupted (harvesting repositories can take days)
- **Statistics & logging**: Detailed logging of fetched URLs and harvesting statistics
- **Docker support**: Runs in a containerized environment for easy deployment (no Python venv necessary)

## Prerequisites

- Docker installed on your system
- An LDES endpoint URL (like https://data.rijksmuseum.nl/ldes/dataset/260242/collection.json which is the [Delfts aardewerk](https://datasetregister.netwerkdigitaalerfgoed.nl/show.php?lang=en&uri=https%3A%2F%2Fid.rijksmuseum.nl%2F260242) event stream from the Rijksmuseum)

## Quick Start

### 1. Build the Docker image

```bash
docker build -t ldes-harvester .
```

### 2. Run the harvester

#### Basic usage

```bash
docker run -v "$(pwd)/cache:/app/cache" ldes-harvester \
  https://example.com/ldes
```

#### Advanced options

```bash
# Disable resume (start fresh)
docker run -v "$(pwd)/cache:/app/cache" ldes-harvester \ 
  --no-resume https://example.com/ldes

# Custom cache directory (inside container)
docker run -v "$(pwd)/my-cache:/data" ldes-harvester \
  --cache-dir /data https://example.com/ldes

# View help
docker run ldes-harvester --help
```

## Output structure

The harvester creates the following structure in the cache directory:

```
cache/
├── state.json            # Resume state (processed pages and members)
├── harvester.log         # Detailed log file
├── <hash1>.nt            # N-Triples file for member 1
├── <hash2>.nt            # N-Triples file for member 2
└── ...
```

### File naming

Each member is saved as a separate N-Triples file with a filename derived from the SHA-256 hash of the member's ID. This ensures:
- Unique filenames for each member
- Deduplication (same member won't be saved twice)
- Consistent naming across runs

## Resume mechanism

The harvester automatically saves its state to `cache/state.json` after every 10 pages. This includes:
- List of processed page URLs
- List of processed member IDs
- Current statistics

If the harvester is interrupted:
1. Simply run the same command again
2. It will automatically resume from the last saved state
3. Already processed members and pages will be skipped

To disable resume and start fresh, use the `--no-resume` flag.

## Statistics

At the end of each run, the harvester displays:
- **Members harvested**: Total number of LDES members saved
- **Pages processed**: Total number of LDES pages fetched
- **Errors encountered**: Number of errors during harvesting
- **Duration**: Total time taken
- **Cache directory**: Location of saved files

## Logging

The harvester provides two levels of logging:

1. **Console Output** (INFO level):
   - URLs being fetched
   - Number of members found on each page
   - Progress updates
   - Final statistics

2. **Log File** (`cache/harvester.log`):
   - Detailed DEBUG level information
   - Complete error traces
   - Timestamps for all operations

## How LDES Works

LDES (Linked Data Event Streams) is a specification for publishing collections of versioned objects:

1. **Entry Point**: The collection URL returns metadata and links to time-based pages
2. **Pagination**: Each page contains members and links to next pages via `relation` objects
3. **Members**: Individual data objects in JSON-LD format with timestamps
4. **Traversal**: The harvester follows all pagination links recursively

**Key Difference from IIIF Change Discovery**: LDES provides events that *embed full object representations* (or object fragments), so the evolving state of an object is delivered directly within the stream without requiring separate dereferencing of object URIs.

## Understanding JSON-LD Context and Ontology Expansion

When you harvest objects and convert them to N-Triples, you may notice that objects published using one ontology (like Linked Art) appear in the N-Triples output using a different ontology (like CIDOC-CRM). This is **not an error** but a fundamental feature of JSON-LD and semantic web technologies.

### Why This Happens

**JSON-LD Context Expansion**: JSON-LD documents include a `@context` that maps short property names to full URIs. When the RDFlib library parses JSON-LD and converts it to N-Triples, it:

1. **Expands** all compact property names to their full URI form
2. **Resolves** all namespace prefixes to complete URIs
3. **Flattens** the nested JSON structure into RDF triples

### Example

A Linked Art object might use compact notation:
```json
{
  "@context": "https://linked.art/ns/v1/linked-art.json",
  "type": "HumanMadeObject",
  "identified_by": [...]
}
```

But in N-Triples, this becomes:
```
<uri> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.cidoc-crm.org/cidoc-crm/E22_Man-Made_Object> .
<uri> <http://www.cidoc-crm.org/cidoc-crm/P1_is_identified_by> ...
```

### Why Linked Art Expands to CIDOC-CRM

**Linked Art is built on CIDOC-CRM**: [Linked Art](https://linked.art/) is a community-developed profile that uses CIDOC-CRM as its underlying ontology. The Linked Art context maps friendly property names (like `identified_by`) to their corresponding CIDOC-CRM properties (like `P1_is_identified_by`).

This means:
- **Linked Art** = Human-friendly JSON-LD with simplified property names
- **CIDOC-CRM** = The formal ontology underneath with full semantic definitions
- **N-Triples** = The expanded, fully-qualified RDF representation

### Benefits of This Approach

1. **Semantic Interoperability**: Different JSON-LD profiles can map to the same underlying ontology
2. **Machine Readability**: N-Triples contain unambiguous, fully-qualified URIs
3. **SPARQL Querying**: The expanded form makes it easy to query across different source formats
4. **Standard Compliance**: CIDOC-CRM URIs are standardized and widely recognized

### What This Means for Your Data

- If you're importing into a triplestore, use the N-Triples directly - they contain the complete semantic information
- If you need human-readable property names, retain the original JSON-LD with its context
- The ontology URIs (CIDOC-CRM, Getty AAT, etc.) in the N-Triples are the "true" semantic representation of your data

## Technical Details

### Dependencies

- **Python 3.11**: Runtime environment
- **requests**: HTTP client for fetching LDES pages
- **rdflib**: RDF parsing and N-Triples serialization

### Architecture

The harvester follows this workflow:

1. Fetch the LDES collection entry point
2. Extract initial page URLs from relations
3. For each page:
   - Fetch and parse JSON-LD content
   - Extract members
   - Convert each member to RDF graph
   - Serialize to N-Triples format
   - Save with hash-based filename
4. Follow pagination links recursively
5. Save state periodically for resume capability

## Development

### Running Without Docker

```bash
# Install dependencies
pip install -r requirements.txt

# Run harvester
python harvester.py --cache-dir ./cache \
  https://data.rijksmuseum.nl/ldes/dataset/260250/collection.json
```

## License

This project is provided as-is for harvesting publicly available LDES endpoints.

## References

- [LDES Specification](https://w3id.org/ldes/specification)
- [Rijksmuseum LDES](https://data.rijksmuseum.nl/docs/ldes/)

## See also

- [IIIF Change Discovery harvester](https://github.com/coret/cd-harvester)
