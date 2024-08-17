from types import ModuleType
from typing import Generator, Sequence, Set, Optional, Union
from dagster import _utils
from dagster._core.definitions.load_assets_from_modules import find_modules_in_package
from dagster._config.pythonic_config import PartialResource, ConfigurableResourceFactory


def _find_in_module(module: ModuleType, definition_type: _utils.IHasInternalInit) -> Generator[_utils.IHasInternalInit, None, None]:
    for attr in dir(module):
        value = getattr(module, attr)
        if isinstance(value, definition_type):
            yield value
        elif isinstance(value, list) and all(isinstance(el, definition_type) for el in value):
            yield from value


def load_from_package_module(package_module: ModuleType, definition_type: _utils.IHasInternalInit) -> Sequence[any]:
    definition_ids: Set[int] = set()
    definitions: Sequence[definition_type] = []
    for module in find_modules_in_package(package_module):
        for definition in _find_in_module(module, definition_type):
            if definition_id := id(definition) not in definition_ids:
                definition_ids.add(definition_id)
                definitions.append(definition)

    return definitions


class DefinedResource:
    def __init__(self, name: Optional[str], resource: Union[PartialResource, ConfigurableResourceFactory]):
        self.name = name
        self.resource = resource


def defined_resource(func, name: Optional[str] = None) -> DefinedResource:
    if not name:
        name = func.__name__

    resource = func()
    if not isinstance(resource, (ConfigurableResourceFactory, PartialResource)):
        raise Exception(f'Invalid return on defined_resource `{name}`. Found: {type(resource)}, Expected ConfigurableResource or ConfigurableIOManagerFactory')
    
    return DefinedResource(name, resource)