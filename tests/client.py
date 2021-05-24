from __future__ import annotations

import os

import elasticsearch


def get_elasticsearch_client(
    elasticsearch_client_fqdn: Optional[str] = os.getenv("ELASTICBUDTEST_CLIENT_FQDN"),
    elasticsearch_client_port: Union[str, int] = os.getenv(
        "ELASTICBUDTEST_CLIENT_PORT", 443
    ),
    elasticsearch_username: Optional[str] = os.getenv("ELASTICBUDTEST_USERNAME"),
    elasticsearch_password: Optional[str] = os.getenv("ELASTICBUDTEST_PASSWORD"),
) -> elasticsearch.Elasticsearch:

    for i, arg in enumerate(
        [
            elasticsearch_client_fqdn,
            elasticsearch_client_port,
            elasticsearch_username,
            elasticsearch_password,
        ]
    ):
        assert arg is not None, f"set {i}: {arg}"

    print(elasticsearch_username)
    elasticsearch_client = elasticsearch.Elasticsearch(
        hosts=[
            {
                "host": elasticsearch_client_fqdn,
                "port": elasticsearch_client_port,
            }
        ],
        timeout=300,
        http_auth=(elasticsearch_username, elasticsearch_password),
        use_ssl=True,
        verify_certs=True,
    )

    return elasticsearch_client
