"""State accountability ETL framework.

State-agnostic infrastructure (runner, log helpers, validator framework) lives at
the top level. State-specific code (handlers, validators, file conventions) lives
in subpackages: tn/, ga/, ca/, etc.

The entry-point scripts (etl/load_tn_accountability.py and equivalents) are thin
CLI wrappers that select the right state subpackage and iterate over raw files.
"""
