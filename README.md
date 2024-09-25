# 🦊 🗃️ FogX-Store 
FogX-Store is a dataset store service that collects and serves large robotics datasets. 

Users of [Fog-X](https://github.com/KeplerC/fog_x/tree/main), machine learning practitioners in robotics, can solely rely on FogX-Store to get datasets from different data source (FogX-Store is the Hub!), and perform analytic workloads without the need to manage the data flow themselves.

<img width="827" alt="截屏2024-09-23 13 14 57" src="https://github.com/user-attachments/assets/d12e0ad2-022c-4c2a-bc84-a384e8726089">

## Developer Setup

### 1. Install Poetry
Follow the [official instruction](https://python-poetry.org/docs/#installing-with-the-official-installer) to install Poetry.

Poetry is a package and dependency managing tool, like pip, but more powerful

### 2. Install dependencies
```
poetry install
```

### 3. Develop inside Poetry Environment

Run
```
poetry shell
```
to start a shell where `python` command bind to the virtualenv created by Poetry, you can then find all the dependencies just installed.
```
$ poetry shell
Spawning shell within /home/ubuntu/.cache/pypoetry/virtualenvs/fogxstore-nGESZMNh-py3.10
. /home/ubuntu/.cache/pypoetry/virtualenvs/fogxstore-nGESZMNh-py3.10/bin/activate
(fogxstore-py3.10)$ which python3
/home/ubuntu/.cache/pypoetry/virtualenvs/fogxstore-nGESZMNh-py3.10/bin/python3
```

## Example

### Run a FogX-Store server

```
python3 examples/example_server.py
```

### Run a FogX-Store client
With in a new shell session, run

```
python3 examples/example_client.py
```

## Test

```
poetry run pytest
```
