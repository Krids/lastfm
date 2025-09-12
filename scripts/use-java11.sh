#!/bin/bash

# Simple script to use Java 11 installed via Homebrew
# Usage: source scripts/use-java11.sh

# Set JAVA_HOME to Homebrew's Java 11
if [ -d "/opt/homebrew/opt/openjdk@11" ]; then
    # Apple Silicon Mac
    export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
elif [ -d "/usr/local/opt/openjdk@11" ]; then
    # Intel Mac
    export JAVA_HOME="/usr/local/opt/openjdk@11"
else
    echo "❌ Java 11 not found in Homebrew locations"
    echo "   Please install with: brew install openjdk@11"
    exit 1
fi

# Add Java to PATH
export PATH="$JAVA_HOME/bin:$PATH"

# Verify Java 11 is active
java_version=$(java -version 2>&1 | head -n 1 | awk -F '"' '{print $2}' | cut -d'.' -f1)

if [ "$java_version" = "11" ]; then
    echo "✅ Java 11 is now active"
    echo "   JAVA_HOME: $JAVA_HOME"
else
    echo "⚠️  Warning: Java version is $java_version, expected 11"
fi
