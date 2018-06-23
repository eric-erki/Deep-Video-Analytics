#!/usr/bin/env sh
python manage.py makemigrations
cp scripts/custom_migration.py dvaapp/migrations/
python manage.py migrate
