project = "opensanctions"
copyright = "2021, OpenSanctions Team"
author = "OpenSanctions Team"

# Do not modify manually
release = "3.0.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
]
templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

intersphinx_mapping = {
    "followthemoney": ("https://followthemoney.readthedocs.io/en/latest/", None),
    "requests": ("https://docs.python-requests.org/en/master/", None),
}

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
