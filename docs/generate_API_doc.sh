#!/bin/sh

poetry run python src/chronos/api/generate_API_doc.py
git add docs
