class AsteriskNotAtListError(KeyError):
    pass


class InvalidSplatError(KeyError):
    pass


class RecursedToKeyError(KeyError):
    pass


class ElasticsearchError(Exception):
    pass


class ElasticsearchUnreachableError(ElasticsearchError):
    pass


class ElasticsearchNotReadyError(ElasticsearchError):
    pass


class MissingTemplateError(ElasticsearchError):
    pass
