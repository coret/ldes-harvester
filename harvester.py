#!/usr/bin/env python3
"""
LDES Harvester - Harvests Linked Data Event Streams and caches members as N-Triples
"""
import argparse
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set
from urllib.parse import urljoin, urlparse

import requests
from rdflib import Graph
from rdflib.exceptions import ParserError


class LDESHarvester:
    """Harvests LDES endpoints and caches members as N-Triples files"""

    def __init__(self, cache_dir: str = "./cache", resume: bool = True):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.state_file = self.cache_dir / "state.json"
        self.resume = resume

        # Statistics
        self.stats = {
            "start_time": datetime.now().isoformat(),
            "members_harvested": 0,
            "pages_processed": 0,
            "errors": 0,
            "total_duration": 0,
        }

        # State management
        self.processed_pages: Set[str] = set()
        self.processed_members: Set[str] = set()
        self.pending_pages: List[str] = []  # Queue of pages to process

        # Setup logging
        self._setup_logging()

        # Load previous state if resuming
        if self.resume:
            self._load_state()

    def _setup_logging(self):
        """Configure logging to console and file"""
        log_format = "%(asctime)s - %(levelname)s - %(message)s"
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(self.cache_dir / "harvester.log")
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _load_state(self):
        """Load previous harvesting state for resume capability"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    self.processed_pages = set(state.get("processed_pages", []))
                    self.processed_members = set(state.get("processed_members", []))
                    self.pending_pages = state.get("pending_pages", [])
                    self.stats = state.get("stats", self.stats)
                    self.logger.info(f"Resumed from previous state: {len(self.processed_members)} members, {len(self.processed_pages)} pages, {len(self.pending_pages)} pending")
            except Exception as e:
                self.logger.error(f"Failed to load state: {e}")
                self.processed_pages = set()
                self.processed_members = set()
                self.pending_pages = []

    def _save_state(self):
        """Save current harvesting state"""
        try:
            state = {
                "processed_pages": list(self.processed_pages),
                "processed_members": list(self.processed_members),
                "pending_pages": self.pending_pages,
                "stats": self.stats,
                "last_updated": datetime.now().isoformat()
            }
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save state: {e}")

    def _fetch_url(self, url: str, retry: int = 3) -> Dict:
        """Fetch URL with retry logic"""
        for attempt in range(retry):
            try:
                self.logger.info(f"Fetching: {url}")
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1}/{retry} failed for {url}: {e}")
                if attempt == retry - 1:
                    self.stats["errors"] += 1
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff

    def _get_member_id(self, member: Dict) -> str:
        """Extract member ID from member object"""
        # Try different possible ID fields
        for id_field in ["@id", "id", "object", "@type"]:
            if id_field in member:
                value = member[id_field]
                if isinstance(value, str):
                    return value
                elif isinstance(value, dict) and "@id" in value:
                    return value["@id"]

        # Fallback: use hash of entire member
        return hashlib.sha256(json.dumps(member, sort_keys=True).encode()).hexdigest()

    def _save_member_as_ntriples(self, member: Dict, context: Dict = None):
        """Convert member to N-Triples and save to cache"""
        try:
            # Extract the actual object data from @graph if present
            # Otherwise use the full member (for non-ActivityStreams LDES)
            graph_data = member.get("@graph")
            if graph_data:
                # Use @graph content (the actual art object)
                jsonld_doc = graph_data
                # Get context from @graph or member level
                if "@context" not in jsonld_doc:
                    jsonld_doc = {"@context": context or member.get("@context"), **graph_data}
            else:
                # Fallback: use entire member
                jsonld_doc = member.copy()
                if context and "@context" not in jsonld_doc:
                    jsonld_doc["@context"] = context

            # Parse as RDF graph
            g = Graph()
            g.parse(data=json.dumps(jsonld_doc), format="json-ld")

            # Generate filename from member ID (use @graph id if available)
            if graph_data and isinstance(graph_data, dict):
                object_id = graph_data.get("id") or graph_data.get("@id") or self._get_member_id(member)
            else:
                object_id = self._get_member_id(member)

            filename = hashlib.sha256(object_id.encode()).hexdigest() + ".nt"
            filepath = self.cache_dir / filename

            # Serialize to N-Triples with UTF-8 encoding
            g.serialize(destination=str(filepath), format="nt", encoding="utf-8")

            self.processed_members.add(object_id)
            self.stats["members_harvested"] += 1
            self.logger.debug(f"Saved object {object_id} to {filename}")

        except ParserError as e:
            self.logger.error(f"Failed to parse member as JSON-LD: {e}")
            self.stats["errors"] += 1
        except Exception as e:
            self.logger.error(f"Failed to save member: {e}")
            self.stats["errors"] += 1

    def _extract_relations(self, data: Dict) -> List[str]:
        """Extract next page URLs from LDES relations"""
        relations = []

        # Check for view relations
        view = data.get("view") or data.get("@view")
        if view:
            if isinstance(view, dict):
                # Direct relation in view
                relation = view.get("relation") or view.get("@relation")
                if relation:
                    relations.extend(self._extract_node_urls(relation))
            elif isinstance(view, list):
                # Multiple views
                for v in view:
                    relation = v.get("relation") or v.get("@relation")
                    if relation:
                        relations.extend(self._extract_node_urls(relation))

        # Check for direct relations at root level
        relation = data.get("relation") or data.get("@relation")
        if relation:
            relations.extend(self._extract_node_urls(relation))

        return relations

    def _extract_node_urls(self, relation) -> List[str]:
        """Extract node URLs from relation objects"""
        urls = []

        if isinstance(relation, dict):
            node = relation.get("node") or relation.get("@node")
            if node:
                if isinstance(node, dict):
                    url = node.get("@id") or node.get("id")
                    if url:
                        urls.append(url)
                elif isinstance(node, str):
                    urls.append(node)
        elif isinstance(relation, list):
            for rel in relation:
                urls.extend(self._extract_node_urls(rel))

        return urls

    def _extract_members(self, data: Dict) -> List[Dict]:
        """Extract members from LDES page"""
        members = []

        # Try different possible member fields
        for member_field in ["member", "members", "@member", "@members"]:
            if member_field in data:
                member_data = data[member_field]
                if isinstance(member_data, list):
                    members.extend(member_data)
                elif isinstance(member_data, dict):
                    members.append(member_data)

        return members

    def _process_page(self, url: str, context: Dict = None):
        """Process a single LDES page"""
        if url in self.processed_pages:
            self.logger.debug(f"Already processed page: {url}, checking for unprocessed next pages")
            # Still need to check for next pages that might not be processed yet
            try:
                data = self._fetch_url(url)
                page_context = data.get("@context", context)
                next_urls = self._extract_relations(data)
                for next_url in next_urls:
                    if next_url not in self.processed_pages and next_url not in self.pending_pages:
                        self.pending_pages.append(next_url)
                        self._process_page(next_url, page_context)
            except Exception as e:
                self.logger.error(f"Failed to extract next pages from {url}: {e}")
            return

        try:
            # Add to pending queue
            if url not in self.pending_pages:
                self.pending_pages.append(url)

            data = self._fetch_url(url)

            # Extract and save context if present
            page_context = data.get("@context", context)

            # Extract and save members
            members = self._extract_members(data)
            self.logger.info(f"Found {len(members)} members on page: {url}")

            for member in members:
                member_id = self._get_member_id(member)
                if member_id not in self.processed_members:
                    self._save_member_as_ntriples(member, page_context)

            # Mark page as processed and remove from pending
            self.processed_pages.add(url)
            if url in self.pending_pages:
                self.pending_pages.remove(url)
            self.stats["pages_processed"] += 1

            # Save state periodically
            if self.stats["pages_processed"] % 10 == 0:
                self._save_state()

            # Extract and process next pages
            next_urls = self._extract_relations(data)
            for next_url in next_urls:
                if next_url not in self.processed_pages and next_url not in self.pending_pages:
                    self._process_page(next_url, page_context)

        except Exception as e:
            self.logger.error(f"Failed to process page {url}: {e}")
            self.stats["errors"] += 1

    def harvest(self, ldes_url: str):
        """Main harvesting method"""
        self.logger.info(f"Starting LDES harvest from: {ldes_url}")
        start_time = time.time()

        try:
            # First, process any pending pages from previous interrupted run
            if self.pending_pages:
                self.logger.info(f"Resuming with {len(self.pending_pages)} pending pages")
                # Make a copy since we'll modify the list during processing
                pending_copy = self.pending_pages.copy()
                for pending_url in pending_copy:
                    if pending_url not in self.processed_pages:
                        self.logger.info(f"Resuming from pending page: {pending_url}")
                        self._process_page(pending_url, None)

                # If we processed all pending pages successfully, we're done
                if not self.pending_pages:
                    self.logger.info("All pending pages processed, harvest complete")
                    self._save_state()
                    self.stats["total_duration"] = time.time() - start_time
                    self.stats["end_time"] = datetime.now().isoformat()
                    self._print_summary()
                    return

            # Fetch the collection entry point
            data = self._fetch_url(ldes_url)
            context = data.get("@context")

            # Check if this is the collection entry point or a page
            if data.get("@type") == "EventStream" or data.get("type") == "EventStream":
                self.logger.info("Detected EventStream collection entry point")
                # Extract initial pages from relations
                initial_urls = self._extract_relations(data)
                self.logger.info(f"Found {len(initial_urls)} initial pages to process")

                for url in initial_urls:
                    self._process_page(url, context)
            else:
                # Treat as a direct page
                self.logger.info("Processing as direct LDES page")
                self._process_page(ldes_url, context)

            # Final state save
            self._save_state()

            # Calculate final statistics
            self.stats["total_duration"] = time.time() - start_time
            self.stats["end_time"] = datetime.now().isoformat()

            # Print summary
            self._print_summary()

        except Exception as e:
            self.logger.error(f"Harvesting failed: {e}")
            self._save_state()
            raise

    def _print_summary(self):
        """Print harvesting statistics"""
        self.logger.info("=" * 60)
        self.logger.info("HARVESTING COMPLETE")
        self.logger.info("=" * 60)
        self.logger.info(f"Members harvested: {self.stats['members_harvested']}")
        self.logger.info(f"Pages processed: {self.stats['pages_processed']}")
        self.logger.info(f"Errors encountered: {self.stats['errors']}")
        self.logger.info(f"Duration: {self.stats['total_duration']:.2f} seconds")
        self.logger.info(f"Cache directory: {self.cache_dir.absolute()}")
        self.logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="LDES Harvester - Harvest Linked Data Event Streams"
    )
    parser.add_argument(
        "url",
        help="LDES endpoint URL to harvest"
    )
    parser.add_argument(
        "--cache-dir",
        default="/app/cache",
        help="Directory to cache N-Triples files (default: /app/cache)"
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable resume capability (start fresh)"
    )

    args = parser.parse_args()

    harvester = LDESHarvester(
        cache_dir=args.cache_dir,
        resume=not args.no_resume
    )

    try:
        harvester.harvest(args.url)
    except KeyboardInterrupt:
        harvester.logger.info("Harvesting interrupted by user")
        harvester._save_state()
        sys.exit(1)
    except Exception as e:
        harvester.logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
