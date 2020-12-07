install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt
lint:
	black ./etl.py

all: install lint
