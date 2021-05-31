from __future__ import annotations
import copy
import json
from pathlib import Path

import elasticsearch
from elasticsearch import Elasticsearch, AsyncElasticsearch
from elasticsearch.helpers import bulk
from tenacity import retry, stop_after_attempt, wait_fixed

from .errors import (
    ElasticsearchUnreachableError,
    ElasticsearchNotReadyError,
)
from .logger import get_logger
from .utils import batch, recurse_splat_key


def check_elasticsearch(
    elasticsearch_client: Elasticsearch,
) -> None:

    log = get_logger("elasticbud.check_elasticsearch")

    host = elasticsearch_client.transport.hosts[0]["host"]
    port = elasticsearch_client.transport.hosts[0]["port"]

    try:
        health = elasticsearch_client.cluster.health()
        version = elasticsearch_client.info()["version"]["number"]
        log.info(
            f"cluster at {host}:{port} is called '{health['cluster_name']}' on {version} and is {health['status']}"
        )
        if health["status"] == "red":
            raise ElasticsearchNotReadyError("cluster is red")
    except elasticsearch.exceptions.ConnectionError as e:
        raise ElasticsearchUnreachableError(
            f"while attempting to connect to elasticsearch at {host}:{port}"
        ) from e


@retry(stop=stop_after_attempt(5), wait=wait_fixed(0.5))
def document_exists(
    elasticsearch_client: Elasticsearch,
    doc: Dict[str, Any],
    index: str,
    identity_fields: List[str],
    delete_if_exists: bool = False,
) -> bool:

    log = get_logger("elasticbud.document_exists")

    query_filters = list()
    for field in identity_fields:
        try:
            query_filters.append(
                {"terms": {field: doc[field]}}
                if isinstance(doc[field], list)
                else {"term": {field: doc[field]}}
            )
        except KeyError:
            # if the doc is missing an identity field, we will index the new document
            return False

    body = {"query": {"bool": {"filter": query_filters}}}

    try:
        resp = elasticsearch_client.search(index=index, body=body)
    except elasticsearch.exceptions.NotFoundError:
        return False

    hits = resp["hits"]["hits"]
    if len(hits) > 0:
        if delete_if_exists:
            if len(hits) > 1:
                log.warning(f"{len(hits)} {index} documents matched the query: {body}")
            for hit in hits:
                log.info(
                    f"deleting existing {index} document matching query (id: {hit['_id']})"
                )
                elasticsearch_client.delete(index=index, id=hit["_id"])
                return False
        else:
            return True
    else:
        return False


def doc_gen(
    elasticsearch_client: Elasticsearch,
    docs: List[Dict[str, Any]],
    index: str,
    identity_fields: Optional[List[str]],
    overwrite: bool,
    quiet: bool = False,
) -> Generator[Dict[str, Any], None, None]:

    log = get_logger("elasticbud.doc_gen")

    if identity_fields is not None:
        # must have manage permission on index to refresh, this is only necessary for idempotent indexing calls
        elasticsearch_client.indices.refresh(index=index, ignore_unavailable=True)

    yielded = 0
    exists = 0
    for doc in docs:
        doc = dict(
            doc
        )  # convert Index classes to plain dictionaries for Elasticsearch API
        if identity_fields is not None and document_exists(
            elasticsearch_client,
            doc,
            index,
            identity_fields,
            delete_if_exists=overwrite,
        ):
            exists += 1
            continue
        doc.update(_index=index)
        yield doc
        yielded += 1
    if not quiet:
        log.info(
            f"{yielded} documents yielded for indexing to {index}"
            + (f" ({exists} already existed)" if exists > 0 else "")
        )


@retry(stop=stop_after_attempt(5), wait=wait_fixed(0.5))
def index_to_elasticsearch(
    elasticsearch_client: Elasticsearch,
    index: str,
    docs: Iterator[Dict[str, Any]],
    identity_fields: Optional[List[str]] = None,
    overwrite: bool = False,
    index_template: Optional[Union[str, Path, Dict[str, Any]]] = None,
    batch_size: Optional[int] = None,
    quiet: bool = False,
) -> None:

    log = get_logger("elasticbud.index_to_elasticsearch")

    if index_template is not None:
        if isinstance(index_template, str) or isinstance(index_template, Path):
            template_body = json.loads(Path(index_template).read_text())
            template_source = f"JSON file {index_template}"
        elif isinstance(index_template, dict):
            template_body = index_template
            template_source = "passed dictionary"
        else:
            raise ValueError(
                f"could not figure out how to treat passed index_template: {index_template}"
            )

        elasticsearch_client.indices.put_template(name=index, body=template_body)
        log.debug(f"applied index template named '{index}' from {template_source}")

    if batch_size is None:
        bulk(
            elasticsearch_client,
            doc_gen(
                elasticsearch_client, docs, index, identity_fields, overwrite, quiet
            ),
        )
    else:
        for docs_batch in batch(docs, n=batch_size):
            bulk(
                elasticsearch_client,
                doc_gen(
                    elasticsearch_client,
                    docs_batch,
                    index,
                    identity_fields,
                    overwrite,
                    quiet,
                ),
            )
    if not quiet:
        log.debug("bulk indexing complete")


@retry(stop=stop_after_attempt(5), wait=wait_fixed(0.5))
def get_response_value(
    elasticsearch_client: Elasticsearch,
    index: str,
    query: Dict[str, Any],
    value_keys: List[str],
    size: int = 0,
    debug: bool = False,
    drop_in: bool = False,
    composite_aggregation_name: Optional[str] = None,
) -> Union[Any, Generator[Any]]:

    log = get_logger("elasticbud.get_response_value")

    query = copy.deepcopy(query)

    if debug:
        log.info(f"retrieving value from query against {index} at {value_keys}")
        print(f"GET /{index}/_search?size={size}\n{json.dumps(query,indent=2)}")

    resp = elasticsearch_client.search(index=index, body=query, size=size)

    if composite_aggregation_name is not None:
        try:
            after_key = resp["aggregations"][composite_aggregation_name]["after_key"]
        except KeyError as exc:
            raise KeyError(
                f"No composite aggregation continuation key found at '{composite_aggregation_name}'"
            ) from exc
        values = 0
        while len(list(recurse_splat_key(resp, value_keys))) > 0:
            if len(list(recurse_splat_key(resp, value_keys))) == 1 and list(
                recurse_splat_key(resp, value_keys)
            ) == [[]]:
                # special case if the composite aggregation is asking to return the buckets without a terminal wildcard
                break
            for value in recurse_splat_key(resp, value_keys):
                yield value
                values += 1

            after_key = resp["aggregations"][composite_aggregation_name]["after_key"]
            query["aggs"][composite_aggregation_name]["composite"].update(
                after=after_key
            )
            resp = elasticsearch_client.search(index=index, body=query, size=size)
        log.debug(f"composite aggregation yielded {values} values")

    else:
        values = [value for value in recurse_splat_key(resp, value_keys)]

        if len(values) == 0:
            values = None
        elif len(values) == 1:
            yield values[0]
        else:
            yield from values

        if debug:
            log.info(
                f"query returned {len(values) if values is not None else 0} values"
            )


def fields_in_hits(hits: Iterator[Dict[str, Any]]) -> List[str]:
    """
    List all unique field names present in a set of hits
    """

    fields = set()
    for hit in hits:
        for field in hit["_source"].keys():
            fields.add(field)

    return list(fields)
