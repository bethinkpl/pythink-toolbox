#!/bin/sh

poetry run cli generate-api-documentation
git add docs
