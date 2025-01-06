# from tests.programs.util import main
import os
import importlib
import ast

from tests.context import cascade
from tests.programs.util import compare_ast, MethodExtracter


target_program_relative_path: str = 'tests/programs/target'
expected_program_relative_path: str = 'tests/programs/expected'

def get_target_file_list():
    target_files: list[str] = os.listdir(target_program_relative_path)
    return list(filter(lambda f: f.endswith('.py'), target_files))

def test_target_programs():
    target_files = get_target_file_list()
    for file_name in target_files:
        cascade.core.clear() # clear cascadeds registerd classes.
        assert not cascade.core.registered_classes, "Registered classes should be empty before importing a Cascade \
                                                        Module"
        # import the module
        import_module_name = file_name.strip('.py')
        importlib.__import__(f'tests.programs.target.{import_module_name}')
        cascade.core.init()
        assert cascade.core.registered_classes, "The Cascade module classes should be registered at this point."
        methods: str = cascade.core.get_compiled_methods()
        print('-' * 100)
        print(f'   COMPILED METHODS FOR: {file_name}')
        print(methods)
        compare_targets_with_expected(file_name, methods)

    assert True

def compare_targets_with_expected(file_name: str, methods: str):
    target_ast: ast.AST = ast.parse(methods)
    expected_ast: ast.AST = ast.parse(open(os.path.join(expected_program_relative_path, file_name)).read())
    target_methods: dict[str, ast.AST] = MethodExtracter.extract(target_ast)
    expected_methods: dict[str, ast.AST] = MethodExtracter.extract(expected_ast)
    for k, target in target_methods.items():
        assert k in expected_methods, f'Method {k} exisits in target but not in expected: {expected_methods.keys()}'
        expected = expected_methods[k]
        assert compare_ast(target, expected)
    
