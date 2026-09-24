#!/bin/bash

for i in {1..5}; do
    echo "$i" | make switch
    make test
done

