#!/usr/bin/env bash

poetry run flake8 lextract tests scripts && ./run_mypy.sh && ./run_tests.sh
