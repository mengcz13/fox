#!/bin/bash
BENCH=/home/mengcz/fox/fox
RESDIR=$BENCH/output
if [ -d "$RESDIR" ]; then
    sudo rm -rf "$RESDIR"
fi
mkdir -p "$RESDIR"


