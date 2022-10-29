from ridant.utils.caching_tools import flatten_dict_for_caching
import json
from ridant.main import RidantCache


SAMPLE = {
    "cart_information": [
        {"item_id": "123455", "quantity": 5},
        {"item_id": "ef5a52520b31", "quantity": 5},
        {"item_id": "43b2", "quantity": 5},
        {"item_id": "2727", "quantity": 5},
        {"item_id": "fffb2e37", "quantity": 5},
    ],
    "cart-metadata": {
        "cart_id": "testing-cart-id",
        "order_start_time": "2019-01-01 00:00:00",
        "restaurant_id": "fffb2e37-2727-43b2-8751-ef5a52520b31",
    },
}

SAMPLE_HEAVILY_NESTED = {
    "nested_one": {
        "child_section_one": {
            "second_level_element": {
                "third_level_element": {
                    "fourth_level_element": {"fifth_level_element": 2}
                }
            }
        }
    },
    "nested_two": {
        "child_section_one": {
            "second_level_element": {
                "third_level_element": {
                    "fourth_level_element": {"fifth_level_element": 2}
                }
            }
        }
    },
    "nested_three": {
        "child_section_one": {
            "second_level_element": {
                "third_level_element": {
                    "fourth_level_element": {"fifth_level_element": 2}
                }
            }
        }
    },
}


def test_flatten_dict_for_caching():
    _commands = flatten_dict_for_caching(SAMPLE)
    assert _commands == {
        "cart_information": '[{"item_id": "123455", "quantity": 5}, {"item_id": "ef5a52520b31", "quantity": 5}, {"item_id": "43b2", "quantity": 5}, {"item_id": "2727", "quantity": 5}, {"item_id": "fffb2e37", "quantity": 5}]',
        "cart-metadata:cart_id": "testing-cart-id",
        "cart-metadata:order_start_time": "2019-01-01 00:00:00",
        "cart-metadata:restaurant_id": "fffb2e37-2727-43b2-8751-ef5a52520b31",
    }


def test_flatten_dict_for_caching_heavily_nested():
    _commands = flatten_dict_for_caching(SAMPLE_HEAVILY_NESTED)
    assert _commands == {
        "nested_one:child_section_one:second_level_element:third_level_element:fourth_level_element:fifth_level_element": 2,
        "nested_two:child_section_one:second_level_element:third_level_element:fourth_level_element:fifth_level_element": 2,
        "nested_three:child_section_one:second_level_element:third_level_element:fourth_level_element:fifth_level_element": 2,
    }


def test_determine_pipeline_commands_needed(ridant_client: RidantCache):
    _results = ridant_client._determine_pipeline_commands_needed(
        base_uid_key="testing_uid", model=SAMPLE
    )
    assert len(_results) == 4
    for item in _results:
        assert item[0][0:12] == "testing_uid:"

def test_determine_pipeline_commands_needed_heavily_nested(ridant_client: RidantCache):
    _results = ridant_client._determine_pipeline_commands_needed(
        base_uid_key="testing_uid", model=SAMPLE_HEAVILY_NESTED
    )
    assert len(_results) == 3
    for item in _results:
        assert item[0][0:12] == "testing_uid:"