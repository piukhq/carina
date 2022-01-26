# Carina

BPL Reward Management API

## configurations

- create a `local.env` file in the root directory
- add your configurations based on the environmental variables required in `app.core.config.Settings` or use the file `local/local.env.example` as template

## running

- `pipenv install --dev`

### api run

- `pipenv run python asgi.py` or `pipenv run uvicorn asgi:app --port=8000`

### reward allocation worker (rq)

- `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES pipenv run python -m app.tasks.worker worker`
- this worker deals with allocating rewards to account_holders, through Polaris' API

> Running the command with the above environment variable is a work around for [this issue](https://github.com/rq/rq/issues/1418). It's a mac only issue to do with os.fork()'ing which rq.Worker utilises.
> 
### reward updates import scheduler

- `pipenv run python -m app.imports.agents.file_agent reward-updates-agent`
- this scheduler deals with downloading reward status change files and inserting into the reward_update table

### reward import scheduler

- `pipenv run python -m app.imports.agents.file_agent reward-import-agent`
- this scheduler deals with downloading reward import files and inserting into the reward table
