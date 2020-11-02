#!/bin/sh

poetry run python src/chronos/api/generate_api_doc.py
git add docs
