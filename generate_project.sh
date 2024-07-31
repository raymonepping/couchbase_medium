#!/bin/bash

# Check if an input file and project name are provided
if [ -z "$1" ] || [ -z "$2" ]; then
  echo "Usage: $0 <input JSON file> <project name>"
  exit 1
fi

INPUT_FILE="$1"
PROJECT_NAME="$2"

# Check if jq and gh are installed
if ! command -v jq &> /dev/null; then
    echo "jq is required to parse JSON. Please install jq."
    exit 1
fi

if ! command -v gh &> /dev/null; then
    echo "GitHub CLI (gh) is required to create repositories. Please install gh."
    exit 1
fi

# Verify the JSON file exists and is readable
if [ ! -f "$INPUT_FILE" ]; then
  echo "Error: The specified JSON file does not exist: $INPUT_FILE"
  exit 1
elif [ ! -r "$INPUT_FILE" ]; then
  echo "Error: The specified JSON file is not readable: $INPUT_FILE"
  exit 1
fi

# Create the root project directory
mkdir -p "$PROJECT_NAME"
cd "$PROJECT_NAME" || exit

# Extract the gitignore path from the JSON file
GITIGNORE_PATH=$(jq -r '.gitignore' "../$INPUT_FILE")

# Copy the gitignore file to the project directory, expanding the tilde to home directory
if [ -n "$GITIGNORE_PATH" ]; then
  if [[ "$GITIGNORE_PATH" == \~/* ]]; then
    GITIGNORE_PATH="${HOME}${GITIGNORE_PATH:1}"
  fi

  if [ -f "$GITIGNORE_PATH" ]; then
    cp "$GITIGNORE_PATH" .gitignore
  else
    echo "Provided gitignore path is invalid or the file does not exist. Creating a blank .gitignore."
    touch .gitignore
  fi
else
  echo "gitignore path is not specified in the JSON file. Creating a blank .gitignore."
  touch .gitignore
fi

# Create root-level files
touch docker-compose.yml .env README.md

# Create shared directory and its contents
mkdir -p shared
jq -c '.shared[]' "../$INPUT_FILE" | while read shared_resource; do
  shared_resource=$(echo "$shared_resource" | jq -r '.')
  if [[ "$shared_resource" == */ ]]; then
    mkdir -p "shared/$shared_resource"
  else
    touch "shared/$shared_resource"
  fi
done

# Create application directories, config files, and initialization scripts
jq -c '.apps[]' "../$INPUT_FILE" | while read app; do
  app_name=$(echo "$app" | jq -r '.name')
  config_file=$(echo "$app" | jq -r '.config')
  init_script=$(echo "$app" | jq -r '.init')

  mkdir -p "$app_name/src"
  touch "$app_name/Dockerfile"
  touch "$app_name/$config_file"
  touch "$app_name/$init_script"
done

# Copy the commit_git.sh script to the project directory
COMMIT_SCRIPT_PATH="/Users/repping/Documents/GitHub/---scripting/Couchbase/Docker/commit_git.sh"
if [ -f "$COMMIT_SCRIPT_PATH" ]; then
  cp "$COMMIT_SCRIPT_PATH" ./commit_git.sh
  echo "commit_git.sh script copied to the project directory."
else
  echo "commit_git.sh script not found at $COMMIT_SCRIPT_PATH."
fi

# Initialize Git repository and make an initial commit
git init
git add .
git commit -m "Initial commit for $PROJECT_NAME"

# Create a new GitHub repository using gh CLI
gh repo create "$PROJECT_NAME" --public --source=. --remote=origin --push

# Rename the default branch to 'main' and push to the remote repository
git branch -m main
git push -u origin main

# Output message
echo "Project structure created successfully under $PROJECT_NAME based on $INPUT_FILE, committed to Git, and pushed to GitHub with 'main' as the default branch."
