#!/bin/bash
echo "Compiling RDT Protocol..."
cd src && javac *.java && echo "✔ Compilation successful!" || echo "✘ Compilation failed."
