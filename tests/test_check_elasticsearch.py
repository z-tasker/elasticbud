from __future__ import annotations

import elasticsearch
import pytest

from elasticbud import check_elasticsearch
from elasticbud.errors import ElasticsearchUnreachableError

from client import get_elasticsearch_client


def test_check_elasticsearch() -> None:
    client = get_elasticsearch_client()

    check_elasticsearch(client)

    bad_client = get_elasticsearch_client(elasticsearch_client_fqdn="not.an.elasticsearch.cluster.fosure")

    with pytest.raises(ElasticsearchUnreachableError):
        check_elasticsearch(bad_client)
