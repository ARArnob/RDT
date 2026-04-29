#!/bin/bash
echo "Compiling RDT Protocol..."
if cd src && javac *.java; then
    echo "✔ Compilation successful!"
    echo "Starting RDT Web Server..."
    java RdtWebServer
else
    echo "✘ Compilation failed."
fi
