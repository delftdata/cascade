import os
import itertools
import ast
import difflib

import importlib


# colors
red = lambda text: f"\033[38;2;255;0;0m{text}\033[38;2;255;255;255m"
green = lambda text: f"\033[38;2;0;255;0m{text}\033[38;2;255;255;255m"
blue = lambda text: f"\033[38;2;0;0;255m{text}\033[38;2;255;255;255m"
white = lambda text: f"\033[38;2;255;255;255m{text}\033[38;2;255;255;255m"


def compare_ast(node1, node2):
    """
        Compares twoo ast's on equality.
    
        sourced from:  https://stackoverflow.com/questions/3312989/elegant-way-to-test-python-asts-for-equality-not-reference-or-object-identity
    """
    if type(node1) is not type(node2):
        return False
    if isinstance(node1, ast.AST):
        for k, v in vars(node1).items():
            if k in ('lineno', 'col_offset', 'ctx', 'end_lineno'):
                continue
            if not compare_ast(v, getattr(node2, k)):
                return False
        return True
    elif isinstance(node1, list):
        return all(itertools.starmap(compare_ast, zip(node1, node2)))
    else:
        return node1 == node2


class MethodExtracter(ast.NodeVisitor):
    
    def __init__(self):
        self.methods: dict[str, ast.AST] = {}

    def visit_FunctionDef(self, node: ast.FunctionDef):
        self.methods[node.name] = node
    
    @classmethod
    def extract(cls, node: ast.AST):
        c = cls()
        c.visit(node)
        return c.methods


def compare_targets_with_expected(file_name: str, methods: str, expected_program_relative_path: str):
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

def get_edits_string(old, new):
    result = ""
    codes = difflib.SequenceMatcher(a=old, b=new).get_opcodes()
    for code in codes:
        if code[0] == "equal": 
            result += white(old[code[1]:code[2]])
        elif code[0] == "delete":
            result += red(old[code[1]:code[2]])
        elif code[0] == "insert":
            result += green(new[code[3]:code[4]])
        elif code[0] == "replace":
            result += (red(old[code[1]:code[2]]) + green(new[code[3]:code[4]]))
    return result
