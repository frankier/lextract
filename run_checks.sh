#!/usr/bin/env bash

poetry run flake8 && ./run_mypy.sh && ./run_tests.sh
