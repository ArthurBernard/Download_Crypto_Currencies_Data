#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-02-13 18:55:21
# @Last modified by: ArthurBernard
# @Last modified time: 2019-08-14 18:38:23

""" Configuration file of documentation. """

# Built-in packages
import os
import sys
from unittest.mock import MagicMock
# import glob

# Third party packages
# from sphinx.ext.autosummary import _import_by_name
# from numpydoc.docscrape import NumpyDocString
# from numpydoc.docscrape_sphinx import SphinxDocString
# import numpydoc.docscrape as np_docscrape
import sphinx

# Check Sphinx version
if sphinx.__version__ < "1.6":
    raise RuntimeError("Sphinx 1.6 or newer required")

needs_sphinx = '1.6'


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

# autosummary_generate = glob.glob("reference/*.rst")

# sys.path.insert(0, os.path.abspath('../..'))
sys.path.append(os.path.abspath('../..'))
sys.path.append(os.path.abspath('../sphinxext'))

extensions = [
    # 'sphinx.ext.autodoc',
    # 'numpydoc',
    # 'sphinx.ext.intersphinx',
    # 'sphinx.ext.coverage',
    # 'sphinx.ext.doctest',
    # 'sphinx.ext.autosummary',
    # 'sphinx.ext.graphviz',
    # 'sphinx.ext.ifconfig',
    # 'matplotlib.sphinxext.plot_directive',
    # 'IPython.sphinxext.ipython_console_highlighting',
    # 'IPython.sphinxext.ipython_directive',
    #
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.coverage',
    'sphinx.ext.mathjax',
    'sphinx.ext.intersphinx',
    'numpydoc',
    # 'scipyoptdoc',
    # 'doi_role',
    'matplotlib.sphinxext.plot_directive',
    #
    # 'sphinx.ext.autodoc',
    # 'sphinx.ext.napoleon',
    # 'sphinx.ext.intersphinx',
    # 'sphinx.ext.coverage',
    # 'sphinx.ext.autosummary',
    #
    # 'numpydoc',
]

# Napoleon settings
# napoleon_google_docstring = False
# napoleon_numpy_docstring = True
# napoleon_include_init_with_doc = True
# napoleon_include_private_with_doc = False
# napoleon_include_special_with_doc = True
# napoleon_use_admonition_for_examples = False
# napoleon_use_admonition_for_notes = False
# napoleon_use_admonition_for_references = False
# napoleon_use_ivar = False
# napoleon_use_param = False
# napoleon_use_rtype = False

project = 'dccd'
copyright = '2017-2019, Arthur Bernard'
author = 'Arthur Bernard'

version = "1.0"
release = "1.0.2"

templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
pygments_style = 'sphinx'  # Style of code source

add_function_parentheses = False  # Parentheses are appended to function
add_module_names = True  # Module names are prepended to all object name

themedir = os.path.join(os.pardir, 'scipy-sphinx-theme', '_theme')
html_theme = 'scipy'
html_theme_path = [themedir]

# USELESS ?
# numpydoc_show_class_members = True
# class_members_toctree = False
# nitpicky = True
# numpydoc_attributes_as_param_list = False

# html_theme = 'scipy-sphinx-theme'  # 'sphinx_rtd_theme'  # Theme of docs
# html_theme_path = ["./_theme/scipy/"]
html_theme_option = {
    'edit_links': True,
    'sidebar': 'right',
    'scipy_org_logo': False,
    'navigation_links': True,
    'rootlinks': [
        (
            'https://github.com/ArthurBernard/Download_Crypto_Currencies_Data',
            'Download_Crypto_Currencies_Data'
        ),
        (
            'https://download-crypto-currencies-data.readthedocs.io',
            'Docs'
        ),
    ]
    # 'display_version': True,
    # 'prev_next_buttons_location': 'both',
    # 'style_external_links': True,
    # 'vcs_pageview_mode': '',
    # 'style_nav_header_background': 'black',
    # Toc options
    # 'collapse_navigation': False,
    # 'sticky_navigation': True,
    # 'navigation_depth': 4,
    # 'includehidden': False,
    # 'titles_only': False,
    # 'github_url': 'https://github.com/ArthurBernard/\
    # Download_Crypto_Currencies_Data',
}
html_sidebars = {'index': ['searchbox.html']}
html_static_path = ['_static']
html_context = {
    "display_github": True,  # Integrate GitHub
    "github_user": "ArthurBernard",  # Username
    "github_repo": "Download_Crypto_Currencies_Data",  # Repo name
    "github_version": "master",  # Version
    "conf_py_path": "/source/",  # Path in the checkout to the docs root
}

autosummary_generate = False
