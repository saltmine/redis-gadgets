# TOOLS
GIT = /usr/bin/git

# REVISION INFO
HOSTNAME := $(shell hostname)
COMMIT := $(shell $(GIT) rev-parse HEAD)
REV_HASH := $(shell $(GIT) log --format='%h' -n 1)
REV_TAGS := $(shell $(GIT) describe --abbrev=0 --tags --always)
BRANCH := $(shell echo $(GIT_BRANCH)|cut -f2 -d"/")
PY_VERSION := $(shell cat setup.py | grep version |cut -f2 -d"=" | sed "s/[,\']//g")
VERSION_JSON = redis_gadgets/version.json
PIP_REQS_FILE = requirements.txt
PIP_HASH_FILE = requirements.hash
PIP_WHEEL_DIR = /tmp/pip_wheel
ENV_DIR = virtualenv
ENV_HASH := $(shell md5sum $(PIP_REQS_FILE) | cut -c1-32)
WORKSPACE = $(shell echo $$WORKSPACE)


all: test build

version:
	@-echo "Building version info in $(VERSION_JSON)"
	@echo "{\n\t\"hash\": \"$(REV_HASH)\"," > $(VERSION_JSON)
	@echo "\t\"version\": \"$(PY_VERSION)\"," >> $(VERSION_JSON)
	@echo "\t\"hostname\": \"$(HOSTNAME)\"," >> $(VERSION_JSON)
	@echo "\t\"commit\": \"$(COMMIT)\"," >> $(VERSION_JSON)
	@echo "\t\"branch\": \"$(BRANCH)\"," >> $(VERSION_JSON)
	@echo "\t\"tags\": \"$(REV_TAGS)\"\n}" >> $(VERSION_JSON)

clean:
	find . -type f -name "*.py[c|o]" -exec rm -f {} \;
	find . -type f -name "*.edited" -exec rm -f {} \;
	find . -type f -name "*.orig" -exec rm -f {} \;
	find . -type f -name "*.swp" -exec rm -f {} \;
	rm -f redis_gadgets/version.json
	rm -rf dist
	rm -rf build

build: clean version
	python setup.py sdist
	python setup.py bdist_wheel

test: clean pep8 nose

virtualenv:
	$(MAKE) checkvenvhash || $(MAKE) buildvirtualenv

buildvirtualenv:
	@test -d $(PIP_WHEEL_DIR) || mkdir $(PIP_WHEEL_DIR)
	@test -d $(ENV_DIR) || mkdir -p $(ENV_DIR)
	@rm -rf $(ENV_DIR)/$(ENV_HASH)
	@echo
	@echo "Initializing VirtualENV"
	@echo
	@virtualenv -q $(ENV_DIR)/$(ENV_HASH)
	@. $(ENV_DIR)/$(ENV_HASH)/bin/activate; pip install -U "pip>=1.5.6" "setuptools==9.1" "wheel>=0.24.0" > /dev/null
	@echo
	@echo "Download and prepping Packages for VirtualENV"
	@echo
	@. $(ENV_DIR)/$(ENV_HASH)/bin/activate; pip install --download $(PIP_WHEEL_DIR) -r $(PIP_REQS_FILE)
	@echo
	@echo "Building VirtualENV Packages"
	@echo
	@. $(ENV_DIR)/$(ENV_HASH)/bin/activate; pip wheel --find-links=$(PIP_WHEEL_DIR) --wheel-dir=$(PIP_WHEEL_DIR) -r $(PIP_REQS_FILE)
	@. $(ENV_DIR)/$(ENV_HASH)/bin/activate; pip install --no-index --find-links=$(PIP_WHEEL_DIR) --use-wheel -r $(PIP_REQS_FILE)
	@echo $(ENV_HASH) > $(ENV_DIR)/$(ENV_HASH)/$(PIP_HASH_FILE)

checkvenvhash:
	@LHASH=`cat $(ENV_DIR)/$(ENV_HASH)/$(PIP_HASH_FILE) 2>/dev/null`; if [ "$(ENV_HASH)" != "$$LHASH" ]; then exit 1; fi
	@echo
	@echo "Valid VirtualENV found with hash of $(ENV_HASH)"
	@echo

nose: virtualenv
	@. $(ENV_DIR)/$(ENV_HASH)/bin/activate; pip install -U "nose==1.3.4" "nosexcover==1.0.7" > /dev/null
	. $(ENV_DIR)/$(ENV_HASH)/bin/activate; nosetests --with-xcoverage --with-xunit \
	--cover-package=redis_gadgets --cover-erase tests/

pep8: virtualenv
	@. $(ENV_DIR)/$(ENV_HASH)/bin/activate; pip install --upgrade pep8 > /dev/null
	. $(ENV_DIR)/$(ENV_HASH)/bin/activate; pep8 redis_gadgets > pep8.xml || true
	@echo
	@echo "Number of violations: $$(cat pep8.xml | wc -l)"; if [ $$(cat pep8.xml | wc -l) -gt 0 ]; then exit 1; fi
	@echo
