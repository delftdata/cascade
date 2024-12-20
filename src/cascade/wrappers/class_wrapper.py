from typing import Any

from cascade.descriptors import ClassDescriptor

class ClassWrapper:
    """Wrapper around a class implementation. Keeps class and Descripter containing parsed class for analysis.
    """

    def __init__(self, cls, class_desc: ClassDescriptor):
        self.cls = cls
        self.class_desc: ClassDescriptor = class_desc
 