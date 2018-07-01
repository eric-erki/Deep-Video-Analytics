#!/usr/bin/env sh
rm dvaapp/migrations/*.py
rm dvaapp/migrations/*.pyc
touch dvaapp/migrations/__init__.py
python manage.py makemigrations
cp scripts/custom_migration.py dvaapp/migrations/
python manage.py migrate
