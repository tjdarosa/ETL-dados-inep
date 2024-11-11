# For executing pyton script (Ubuntu 22.04):

### unzip and move data 
- unzip ```microdados_censo_escolar_2023.zip``` file
- find ```microdados_ed_basica_2023.csv``` file
- move it to ```./data/``` directory 

### creating virtual environment
```python3 -m venv env```

### activating virtual environment
```source env/bin/activate```

### installing requirements ons virtual environment
```pip install -r ./src/requirements.txt```

### running script 
```python3 ./src/etl.py```