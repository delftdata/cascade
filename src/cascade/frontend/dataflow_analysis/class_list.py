from .class_wrapper import ClassWrapper

class ClassList(list):

    def get_class_by_name(self, name: str) -> ClassWrapper:
        for c in self.__iter__():
            if c.name == name:
                return c
