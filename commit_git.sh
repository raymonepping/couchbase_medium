#!/bin/bash

# Navigate to the project directory
cd ./ || exit

# Get the current date
DD=$(date +'%d')
MM=$(date +'%m')
YYYY=$(date +'%Y')

# Define the commit message with the current date
COMMIT_MESSAGE="$DD/$MM/$YYYY - Updated configuration and fixed bugs"

# Check the status of your working directory
git status

# Stage all changes
git add .

# Commit the changes with the dynamically generated message
git commit -m "$COMMIT_MESSAGE"

# Push changes to the 'master' branch on the remote repository
git push origin main