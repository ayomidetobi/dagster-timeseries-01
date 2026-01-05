#!/bin/bash
# Helper script to run vulture with exclusions from .vulture file

# Read exclude patterns from .vulture file (skip comments and empty lines)
EXCLUDE_PATTERNS=$(grep -v '^#' .vulture | grep -v '^$' | tr '\n' ',' | sed 's/,$//')

# Run vulture with exclusions
vulture --exclude "$EXCLUDE_PATTERNS" "$@"

