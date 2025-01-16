import os

import pytest 
import cascade
import sys


from tests.programs.util import compare_targets_with_expected


target_program_relative_path: str = 'test_programs/target'
expected_program_relative_path: str = 'test_programs/expected'


def get_target_file_list():
    target_files: list[str] = os.listdir(target_program_relative_path)
    return list(filter(lambda f: f.endswith('.py') and '__init__' not in f, target_files))

target_files: list[str] = get_target_file_list()

@pytest.mark.parametrize("file_name", target_files)
def test_target_programs(file_name: str):
    for key in list(sys.modules.keys()):
        if key.startswith("test_programs"):
            del sys.modules[key] 
        
    cascade.core.clear() # clear cascadeds registerd classes.
    assert not cascade.core.registered_classes, "Registered classes should be empty before importing a Cascade \
                                                    Module"
    # import the module
    import_module_name: str = f'test_programs.target.{file_name.strip(".py")}'
    exec(f'import {import_module_name}')
    
    assert cascade.core.registered_classes, "The Cascade module classes should be registered at this point."
    methods: str = cascade.core.get_compiled_methods()
    compare_targets_with_expected(file_name, methods, expected_program_relative_path)
