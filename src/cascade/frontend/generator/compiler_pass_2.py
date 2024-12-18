from cascade.frontend.generator.split_function import SplitFunction


class CompilerPass2:
    """ Adds targets and values to split functions
    """
    
    def __init__(self, split_functions: list[SplitFunction]):
        self.split_functions = split_functions
    
    def extract_targets_and_values(self):
        pass