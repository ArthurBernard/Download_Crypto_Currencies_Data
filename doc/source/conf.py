#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-02-13 18:55:21
# @Last modified by: ArthurBernard
# @Last modified time: 2019-11-07 17:04:27

""" Configuration file of documentation. """

# Built-in packages
import os
import sys
from unittest.mock import MagicMock
from datetime import date
import re
# import glob

needs_sphinx = '7.0'


class Mock(MagicMock):
    @classmethod
    def __getattr__(cls, name):
        return MagicMock()


# --------------------------------------------------------------------------- #
#                           General configuration                             #
# --------------------------------------------------------------------------- #

# np_docscrape.ClassDoc.extra_public_methods = [  # should match class.rst
#    '__call__', '__mul__', '__getitem__', '__len__',
# ]

sys.path.append(os.path.abspath('../..'))
sys.path.append(os.path.abspath('../sphinxext'))

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.coverage',
    'sphinx.ext.doctest',
    'sphinx.ext.viewcode',
    'sphinx.ext.mathjax',
    'sphinx.ext.intersphinx',
    'numpydoc',
    'sphinx_design',
    'sphinx_copybutton',
    'sphinx_click',
    'sphinxcontrib.autodoc_pydantic',
]

# autodoc-pydantic: render config models as field references, not method dumps.
autodoc_pydantic_model_show_json = False
autodoc_pydantic_model_show_config_summary = False
autodoc_pydantic_model_show_validator_summary = False
autodoc_pydantic_model_show_validator_members = False
autodoc_pydantic_model_member_order = 'bysource'
autodoc_pydantic_field_list_validators = False
autodoc_pydantic_field_show_constraints = True
autodoc_pydantic_model_show_field_summary = True

# sphinx-copybutton: strip prompts when copying code blocks
copybutton_prompt_text = r">>> |\.\.\. |\$ "
copybutton_prompt_is_regexp = True

# sphinx.ext.doctest: make the common public API available to every example
# (numpy-style — examples stay focused and still run in CI).
doctest_global_setup = """
import asyncio, os, tempfile
from dccd import Client
from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType
from dccd.domain.records import OHLCBar, Trade, OrderBookSnapshot, OrderBookLevel
from dccd.domain.dataset import DatasetId, Provenance
from dccd.domain.capability import Capability
from dccd.domain.transforms import aggregate_ohlc
from dccd.storage.parquet import ParquetStore
from dccd.storage.runs_sqlite import RunsStore
from dccd.sources.registry import SourceRegistry
"""

project = 'Download Crypto Currencies Data'
copyright = '2017-{}, Arthur Bernard'.format(date.today().year)
author = 'Arthur Bernard'

# The default replacements for |version| and |release|, also used in various
# other places throughout the built documents.
import dccd
version = re.sub(r'\.dev-.*$', r'.dev', dccd.__version__)
release = dccd.__version__

templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
pygments_style = 'sphinx'  # Style of code source

add_function_parentheses = False
add_module_names = False

# --------------------------------------------------------------------------- #
#                                HTML config                                  #
# --------------------------------------------------------------------------- #

html_theme = 'furo'
html_theme_options = {
    "source_repository": "https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/",
    "source_branch": "master",
    "source_directory": "doc/source/",
    "light_logo": "logo-light-transparent.svg",
    "dark_logo": "logo-dark-transparent.svg",
}
html_title = '{} v{} Reference Guide'.format(project, version)
html_static_path = ['_static']
html_css_files = ['custom.css']

html_sidebars = {
    "**": [
        "sidebar/scroll-start.html",
        "sidebar/search.html",
        "sidebar/navigation.html",
        "sidebar/ethical-ads.html",
        "sidebar/related-projects.html",
        "sidebar/scroll-end.html",
    ]
}

html_context = {
    "display_github": True,
    "github_user": "ArthurBernard",
    "github_repo": "Download_Crypto_Currencies_Data",
    "github_version": "master",
    "conf_py_path": "/doc/source/",
}

html_domain_indices = True
html_copy_source = False
html_file_suffix = '.html'

# --------------------------------------------------------------------------- #
#                             Intersphinx config                              #
# --------------------------------------------------------------------------- #

intersphinx_mapping = {
    'python': ('https://docs.python.org/dev', None),
    'polars': ('https://docs.pola.rs/api/python/stable/', None),
    'pydantic': ('https://docs.pydantic.dev/latest/', None),
    'fynance': ('https://fynance.readthedocs.io/en/latest/', None),
}

# --------------------------------------------------------------------------- #
#                             Autosummary config                              #
# --------------------------------------------------------------------------- #

autosummary_generate = True
# autosummary_generate = glob.glob("reference/*.rst")

# --------------------------------------------------------------------------- #
#                               Autodoc config                                #
# --------------------------------------------------------------------------- #

autodoc_default_options = {}
autodoc_inherit_docstrings = False
autodoc_typehints = 'none'

# --------------------------------------------------------------------------- #
#                         Autodoc skip-member hook                            #
# --------------------------------------------------------------------------- #

import enum as _enum

import pydantic as _pydantic

# Members inherited from third-party / builtin bases are noise on our pages
# (Pydantic internals, str/Enum methods like ``maketrans`` on str-based enums).
_INHERITED_NOISE = (
    frozenset(dir(_pydantic.BaseModel))
    | frozenset(dir(str))
    | frozenset(dir(_enum.Enum))
)


def _skip_pydantic_member(app, what, name, obj, skip, options):
    """ Skip members inherited from Pydantic/str/Enum bases (broken or noisy RST). """
    if skip:
        return True
    if name in _INHERITED_NOISE and name not in ('__init__', '__doc__'):
        return True
    return skip


def setup(app):
    app.connect('autodoc-skip-member', _skip_pydantic_member)

# --------------------------------------------------------------------------- #
#                              Numpydoc config                                #
# --------------------------------------------------------------------------- #

# Disable numpydoc's auto-generated method tables to avoid stub-file warnings.
numpydoc_show_class_members = False
numpydoc_class_members_toctree = False

# Suppress citation duplicate warnings from autosummary
suppress_warnings = ['ref.citation']
