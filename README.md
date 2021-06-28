# Carina

BPL Voucher Management API

## configurations

- create a `local.env` file in the root directory
- add your configurations based on the environmental variables required in `app.core.config.Settings`

## running

- `pipenv install --dev`

### api run

- `pipenv run python asgi.py` or `pipenv run uvicorn asgi:app --port=8000`
