# Pyspark Kickstarter

## Quickstart
1. Download the [PyCharm Community Edition](https://www.jetbrains.com/pycharm/download/)
2. Download [pyenv](https://github.com/pyenv/pyenv#installation)
3. Use pyenv to install the latest Python 3.9 version (e.g. `pyenv install 3.9.13`)
4. [Install requirements.txt using Pycharm](#install-requirementstxt-using-pycharm)
5. [Set the Test Configuration](#set-the-test-configuration)
6. Right click on the `src` dir, select `Mark directory as`, select `Sources root`
7. Right click on the `test` dir, select `Mark directory as`, select `Test sources root`
8. Run the tests by selecting the `test` dir and pressing control + shift + r

## Install requirements.txt using Pycharm:
From: [Jetbrains](https://www.jetbrains.com/help/pycharm/managing-dependencies.html#configure-requirements)
1. Press ⌘ , to open the IDE settings and select Tools | Python Integrated Tools.
2. In the Package requirements file field, type the name of the requirements file or click the browse button and locate the desired file.
3. Click OK to save the changes.

## Set the Test configuration
1. Navigate to Edit Configurations... >  Edit Configuration Templates... Python > AutoDetect
2. Add to *Environment Variables*: `PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.9;PYSPARK_PYTHON=/usr/local/bin/python3.9`
3. Make sure interpreter for Python 3.9 is set

## Build and Test Locally
### Test

```bash
cd app
pip install -r requirements.txt -e .
pytest
```

### Build
```bash
cd app
python setup.py bdist_egg
```
          