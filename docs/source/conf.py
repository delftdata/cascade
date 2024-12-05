# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join('..', '..', 'src')))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'cascade'
copyright = '2024, DIS TUDelft'
author = 'DIS TUDelft'
release = '0.1'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.duration',      # Duration report on `make html`
    'sphinx.ext.doctest',       # Enable doctests
    'sphinx.ext.autodoc',       # Document using python docstrings
    'sphinx.ext.napoleon',      # Support for NumPy and Google style docstrings
    # 'sphinx.ext.autosummary',       # Add links to highlighted source code https://www.sphinx-doc.org/en/master/usage/extensions/viewcode.html 
    'sphinx.ext.viewcode'       # Add links to highlighted source code https://www.sphinx-doc.org/en/master/usage/extensions/viewcode.html 
]

templates_path = ['_templates']
exclude_patterns = []



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'furo'
html_static_path = ['_static']

