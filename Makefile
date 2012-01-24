
venv/.ok:
	virtualenv --no-site-packages venv
	./venv/bin/pip install puka
	touch ./venv/.ok

clean:
	rm -rf venv