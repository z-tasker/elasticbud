from __future__ import annotations

import json
from pathlib import Path

import elasticsearch
import pytest

from elasticbud import (
    async_check_elasticsearch,
    async_document_exists,
    async_index_to_elasticsearch,
    async_get_response_value,
    async_fields_in_hits,
)
from elasticbud.errors import ElasticsearchUnreachableError

from client import get_async_elasticsearch_client

TEST_INDEX_NAME = "elasticbud-test-data"


@pytest.mark.asyncio
@pytest.mark.depends(name="test_async_check_elasticsearch")
async def test_async_check_elasticsearch() -> None:
    client = get_async_elasticsearch_client()

    await async_check_elasticsearch(client)

    bad_client = get_async_elasticsearch_client(
        elasticsearch_client_fqdn="not.an.elasticsearch.cluster.fosure"
    )

    with pytest.raises(ElasticsearchUnreachableError):
        await async_check_elasticsearch(bad_client)

    await client.close()
    await bad_client.close()


@pytest.mark.asyncio
@pytest.mark.depends(on=["test_async_check_elasticsearch"])
async def test_async_index_to_elasticsearch() -> None:
    client = get_async_elasticsearch_client()

    docs = json.loads(
        Path(__file__).parent.joinpath(f"{TEST_INDEX_NAME}.json").read_text()
    )

    if await client.indices.exists(index=TEST_INDEX_NAME):
        await client.indices.delete(index=TEST_INDEX_NAME)

    index_template = json.loads(
        Path(__file__).parent.joinpath(f"{TEST_INDEX_NAME}.template.json").read_text()
    )

    # fresh naive indexing operation
    await async_index_to_elasticsearch(
        elasticsearch_client=client,
        index=TEST_INDEX_NAME,
        index_template=index_template,
        docs=docs[:500],  # first 500 docs in
    )

    # dirty indexing operation with idempotency
    await async_index_to_elasticsearch(
        elasticsearch_client=client,
        index=TEST_INDEX_NAME,
        docs=docs[300:],  # from the 300th document onward (200 already exist)
        identity_fields=["date", "article"],
        batch_size=300,  # test batch size customization
    )

    await client.indices.refresh(index=TEST_INDEX_NAME)

    assert int(
        (await client.cat.count(TEST_INDEX_NAME, params={"format": "json"}))[0]["count"]
    ) == len(docs)

    applied_mapping = await client.indices.get_mapping(TEST_INDEX_NAME)
    source_mapping = {TEST_INDEX_NAME: {"mappings": index_template["mappings"]}}

    assert applied_mapping == source_mapping

    await client.close()


@pytest.mark.asyncio
@pytest.mark.depends(on=["test_async_index_to_elasticsearch"])
async def test_async_document_exists() -> None:
    client = get_async_elasticsearch_client()

    docs = json.loads(
        Path(__file__).parent.joinpath(f"{TEST_INDEX_NAME}.json").read_text()
    )

    assert await async_document_exists(
        elasticsearch_client=client,
        doc=docs[101],
        index=TEST_INDEX_NAME,
        identity_fields=["date", "article"],
    )

    assert not await async_document_exists(
        elasticsearch_client=client,
        doc={
            "article": "Elasticbud is the best!",
            "date": "2025-01-10",
            "rank": 1,
            "views": 1000000,
        },
        index=TEST_INDEX_NAME,
        identity_fields=["date", "article"],
    )

    await client.close()


@pytest.mark.asyncio
@pytest.mark.depends(on=["test_async_index_to_elasticsearch"])
async def test_async_get_response_value_plain_aggregation() -> None:
    client = get_async_elasticsearch_client()

    top_5 = list()
    async for hit in async_get_response_value(
        elasticsearch_client=client,
        index=TEST_INDEX_NAME,
        query={
            "query": {"match_all": {}},
            "aggs": {
                "top_pages": {
                    "terms": {
                        "field": "article",
                        "order": {"views": "desc"},
                        "size": 5,
                    },
                    "aggs": {"views": {"avg": {"field": "views"}}},
                }
            },
        },
        value_keys=["aggregations", "top_pages", "buckets"],
    ):
        top_5.extend(hit)

    expected_top_5 = [
        {"key": "Main_Page", "doc_count": 3, "views": {"value": 5854604.666666667}},
        {
            "key": "Kamala_Harris",
            "doc_count": 3,
            "views": {"value": 1704162.3333333333},
        },
        {
            "key": "Special:Search",
            "doc_count": 3,
            "views": {"value": 1317504.3333333333},
        },
        {"key": "Shyamala_Gopalan", "doc_count": 1, "views": {"value": 807215.0}},
        {"key": "Douglas_Emhoff", "doc_count": 1, "views": {"value": 759613.0}},
    ]

    assert top_5 == expected_top_5

    await client.close()


@pytest.mark.asyncio
@pytest.mark.depends(on=["test_async_index_to_elasticsearch"])
async def test_async_get_response_value_wildcard() -> None:
    client = get_async_elasticsearch_client()

    top_5_view_counts = list()
    async for hit in async_get_response_value(
        elasticsearch_client=client,
        index=TEST_INDEX_NAME,
        query={
            "query": {"match_all": {}},
            "aggs": {
                "top_pages": {
                    "terms": {
                        "field": "article",
                        "order": {"views": "desc"},
                        "size": 5,
                    },
                    "aggs": {"views": {"avg": {"field": "views"}}},
                }
            },
        },
        value_keys=["aggregations", "top_pages", "buckets", "*", "views", "value"],
    ):
        top_5_view_counts.append(hit)

    expected_top_5_view_counts = [
        5854604.666666667,
        1704162.3333333333,
        1317504.3333333333,
        807215.0,
        759613.0,
    ]

    assert top_5_view_counts == expected_top_5_view_counts

    await client.close()


@pytest.mark.asyncio
@pytest.mark.depends(on=["test_async_index_to_elasticsearch"])
async def test_async_get_response_value_composite_aggregation() -> None:
    client = get_async_elasticsearch_client()

    query = {
        "query": {"match_all": {}},
        "aggs": {
            "all_articles_avg_views": {
                "composite": {
                    "size": 10,
                    "sources": [
                        {"article": {"terms": {"field": "article"}}},
                        {"date": {"terms": {"field": "date"}}},
                    ],
                },
                "aggregations": {"avg_views": {"avg": {"field": "views"}}},
            }
        },
    }

    all_articles_avg_views_bucket_sets = [
        key
        async for key in async_get_response_value(
            elasticsearch_client=client,
            index=TEST_INDEX_NAME,
            query=query,
            composite_aggregation_name="all_articles_avg_views",
            value_keys=["aggregations", "all_articles_avg_views", "buckets"],
        )
    ]

    assert len(all_articles_avg_views_bucket_sets) == 90

    all_articles_avg_views_buckets = [
        key
        async for key in async_get_response_value(
            elasticsearch_client=client,
            index=TEST_INDEX_NAME,
            query=query,
            composite_aggregation_name="all_articles_avg_views",
            value_keys=["aggregations", "all_articles_avg_views", "buckets", "*"],
        )
    ]

    assert len(all_articles_avg_views_buckets) == 900

    await client.close()


@pytest.mark.asyncio
@pytest.mark.depends(on=["test_async_index_to_elasticsearch"])
async def test_async_fields_in_hits_from_cluster() -> None:
    client = get_async_elasticsearch_client()

    unique_fields = await async_fields_in_hits(
        async_get_response_value(
            elasticsearch_client=client,
            index=TEST_INDEX_NAME,
            query={"query": {"match_all": {}}},
            value_keys=["hits", "hits", "*"],
            debug=True,
            size=100,
        )
    )

    assert sorted(unique_fields) == ["article", "date", "rank", "views"]

    await client.close()
