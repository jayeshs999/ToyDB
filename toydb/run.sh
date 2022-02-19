#!/bin/bash

cd pflayer
make clean
make

cd ../amlayer
make clean
make

cd ../dblayer
make clean
make

echo -e '\n\n\n\n\n\n'

$1
