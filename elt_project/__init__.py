# This file makes the 'elt_project' directory a Python package.
# The main Definitions object, which Dagster loads, is located in the
# elt_project/elt_project/definitions.py file.
#
# When you run `dagster dev -m elt_project.elt_project.definitions`, Dagster
# looks for a variable named `defs` inside that specific module.
# This top-level __init__.py should generally be empty or contain
# package-level initialization code, but not the main Dagster definitions
# if they are housed in a submodule.
pass