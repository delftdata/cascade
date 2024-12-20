from tests.context import cascade

from tests.test_programs.target.checkout_item import User
from cascade.wrappers import ClassWrapper

def test_code_generator():
    classes: list[ClassWrapper] = cascade.core.registered_classes
    assert classes