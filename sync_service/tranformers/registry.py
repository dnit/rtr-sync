from typing import Dict, Tuple, Type
from .transformers import BaseCRMTransformer, CRMTransformer

_TRANSFORMER_REGISTRY: Dict[Tuple[str, str], Type[BaseCRMTransformer]] = {
    ("salesforce", "contact"): CRMTransformer,
    ("hubspot",   "contact"): CRMTransformer,
}



def get_transformer(org_id: str, provider: str, object_type: str) -> BaseCRMTransformer:
    """
    This should be based off some config value mapping from org based field mappings per object_type ,
    If not provided fallback to some logic like this.
    """
    key = (provider.lower(), object_type.lower())
    if key not in _TRANSFORMER_REGISTRY:
        raise ValueError(f"No transformer for {key}")
    return _TRANSFORMER_REGISTRY[key]()