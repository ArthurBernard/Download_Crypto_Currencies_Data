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
    'sphinx.ext.viewcode',
    'sphinx.ext.mathjax',
    'sphinx.ext.intersphinx',
    'numpydoc',
    'sphinx_design',
    'sphinx_copybutton',
]

# sphinx-copybutton: strip prompts when copying code blocks
copybutton_prompt_text = r">>> |\.\.\. |\$ "
copybutton_prompt_is_regexp = True

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
}
html_title = '{} v{} Reference Guide'.format(project, version)
html_static_path = ['_static']
html_css_files = ['custom.css']

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

import pydantic as _pydantic

_PYDANTIC_BASE_MEMBERS = frozenset(dir(_pydantic.BaseModel))


def _skip_pydantic_member(app, what, name, obj, skip, options):
    """ Skip Pydantic BaseModel methods — their docstrings contain broken RST. """
    if skip:
        return True
    if name in _PYDANTIC_BASE_MEMBERS and name not in ('__init__', '__doc__'):
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
