import flatdict
import json


def flatten_dict_for_caching(d: dict) -> dict:
    """Flatten a dictionary for caching purposes.

    Args:
        d (dict): Dictionary to flatten.

    Returns:
        dict: flattened dictonary. in the format of 
        ```"parent:child": "value"
        ```
    """    
    _parse_flattened_dict = flatdict.FlatDict(d)
    _returned_dict = {}
    for key, value in _parse_flattened_dict.items():
        if isinstance(value, (list, dict, set, tuple)):
            _returned_dict[key] = json.dumps(value)
        else:
            _returned_dict[key] = value
    return _returned_dict