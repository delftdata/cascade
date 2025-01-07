import os
import importlib
import ast

from tests.context import cascade
from tests.programs.util import compare_ast, MethodExtracter, get_edits_string


target_program_relative_path: str = 'tests/programs/target'
expected_program_relative_path: str = 'tests/programs/expected'


# TODO: Add df comparison

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
    expected_methods_as_string: str = open(os.path.join(expected_program_relative_path, file_name)).read()
    expected_ast: ast.AST = ast.parse(expected_methods_as_string)
    target_methods: dict[str, ast.AST] = MethodExtracter.extract(target_ast)
    expected_methods: dict[str, ast.AST] = MethodExtracter.extract(expected_ast)
    for k, target in target_methods.items():
        assert k in expected_methods, f'Method {k} exisits in target but not in expected: {expected_methods.keys()}'
        expected = expected_methods[k]
        assert compare_ast(target, expected), get_error_string(file_name, k, methods, expected_methods_as_string, target, expected)

def get_error_string(file_name: str, method_name: str, target_methods: str, expected_methods: str, target: ast.FunctionDef, expected: ast.FunctionDef):
    target_method_as_string: str = '\n'.join(list(target_methods.split('\n'))[target.lineno-1: target.end_lineno])
    expected_as_string: str =  '\n'.join(list(expected_methods.split('\n'))[expected.lineno-1: expected.end_lineno])
    edits_string = get_edits_string(target_method_as_string, expected_as_string)
    return f'compiled is not as expected in file {file_name} for method {method_name}, \n {edits_string}'
