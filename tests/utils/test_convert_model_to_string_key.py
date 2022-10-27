

from ridant.utils.convert_model_to_string_key import to_snake_case, get_name_from_model
from pydantic import BaseModel
from odmantic import Model


class ExamplePydanticModel(BaseModel):
    sample_key: str
    
    
class ExampleOdanticModel(Model):
    sample_key: str
    

class ExamplePydanticModelWithConfig(BaseModel):
    class Config:
        cacheable_group_name = "CustomGroupNamePydnaticModel"
    sample_key: str

class ExampleOdmanticModelWithConfig(BaseModel):
    class Config:
        cacheable_group_name = "CustomGroupNameOdmanticModel"
    sample_key: str


def test_to_snake_case():
    assert to_snake_case("ExamplePydanticModel") == "example_pydantic_model"
    assert to_snake_case("ExampleOdanticModel") == "example_odantic_model"

def test_get_name_from_model():
    assert get_name_from_model(ExamplePydanticModel) == "example_pydantic_model", "Pydantic model name should be converted to snake case"
    assert get_name_from_model(ExampleOdanticModel) == "example_odantic_model", "Odantic model name should be converted to snake case"
    assert get_name_from_model(ExamplePydanticModelWithConfig) == "CustomGroupNamePydnaticModel", "Pydantic model name should from Config"
    assert get_name_from_model(ExampleOdmanticModelWithConfig) == "CustomGroupNameOdmanticModel", "Odmantic model name should from Config"