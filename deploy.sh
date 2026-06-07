#!/bin/bash

SOURCE="/Users/romanoskolkov/Documents/projects/can2HAmqtt"
TARGET="/Volumes/addons/can2HAmqtt"

# Удаляем целевую папку
rm -rf "$TARGET"

# Создаём временный архив (исключая ненужное)
cd "$SOURCE" || exit
tar --exclude='.venv' \
    --exclude='.git' \
    --exclude='__pycache__' \
    --exclude='.DS_Store' \
    --exclude='*.pyc' \
    --exclude='.vscode' \
    --exclude='.github' \
    --exclude='*.egg-info' \
    -cf /tmp/project.tar .

# Создаём целевую папку и распаковываем
mkdir -p "$TARGET"
cd "$TARGET" || exit
tar -xf /tmp/project.tar

# Чистим за собой
rm /tmp/project.tar

echo "✅ Скопировано через tar (без ошибок)"