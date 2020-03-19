#!/bin/bash

# get misspell from https://github.com/client9/misspell

find . -type d \( -path ./build -o -path .git \) -prune -o -name '*.h' -exec misspell -w '{}' \; &
find . -type d \( -path ./build -o -path .git \) -prune -o -name '*.c' -exec misspell -w '{}' \; &

wait

