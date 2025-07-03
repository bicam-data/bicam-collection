import importlib
from functools import lru_cache

import pytest

from bicam_collection.core.schema_config import SchemaManager
from bicam_collection.scrapers.raw_fetchers import get_fetchers


# Load congressional schema once – tests run fast
@lru_cache  # memoise between param iterations
def _load_schema():
    sm = SchemaManager(config_dir="configs")
    return sm.load_schema("congressional")


cong_schema = _load_schema()


@pytest.mark.parametrize(
    "data_type_schema",
    [dt for dt in cong_schema.data_types if dt.related_fields],
    ids=[dt.name for dt in cong_schema.data_types if dt.related_fields],
)
def test_related_fields_have_fetcher_or_method(data_type_schema):
    parent_type = data_type_schema.name
    related_fields = data_type_schema.related_fields

    # Discover fetchers registered for this parent
    fetcher_map = get_fetchers(parent_type)

    # Attempt to locate the PyCongress model class for reflection
    model_cls = None
    try:
        # common plural -> singular mapping: strip trailing "s" if present
        candidate_name = (
            parent_type[:-1].capitalize()
            if parent_type.endswith("s")
            else parent_type.capitalize()
        )
        components_mod = importlib.import_module("pycon.congress.components")
        model_cls = getattr(components_mod, candidate_name, None)
    except ModuleNotFoundError:
        pass  # library missing in env – treat as no class

    for rel in related_fields:
        has_fetcher = rel in fetcher_map
        has_method = False
        if model_cls is not None:
            method_name = f"get_{rel}"
            has_method = hasattr(model_cls, method_name) and callable(
                getattr(model_cls, method_name)
            )

        assert has_fetcher or has_method, (
            f"Related field '{rel}' for parent '{parent_type}' is not backed by a raw fetcher "
            "and no get_{rel}() method on the PyCongress model. Update fetchers or YAML."
        )
