# Define Makefile
ACTIVATE_ENV := source venv/bin/activate
PYTHON = python3

# Default target
.PHONY: install
.PHONY: run
# run is a phony target
# Does not corresopond to a file, but is a command.

# Command to install packages from requirements:
install:
	@echo "Installing packges from requirements.txt........"
	@$(PYTHON) -m pip install -r requirements.txt
	@echo "Installation complete!"
	@echo "Allowing scripts to run"
	chmod +x setup.sh
	chmod +x remove.sh

setup:
	./setup.sh

remove:
	./remove.sh

m1:
	brew install kreuzwerker/taps/m1-terraform-provider-helper
	m1-terraform-provider-helper activate
	m1-terraform-provider-helper install hashicorp/template -v v2.2.0