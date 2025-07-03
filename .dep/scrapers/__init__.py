from contextlib import suppress
from importlib import import_module

# Ensure fetcher modules are imported so their @register decorators run
for _mod in (
    ".bill_fetchers",
    ".amendment_fetchers",
    ".treaty_fetchers",
    ".committeemeeting_fetchers",
    ".committeereport_fetchers",
    ".committeeprint_fetchers",
    ".nomination_fetchers",
    ".hearing_fetchers",
    ".committees_fetchers",
):
    with suppress(ModuleNotFoundError):  # optional modules may be absent in some envs
        import_module(_mod, package=__name__)
