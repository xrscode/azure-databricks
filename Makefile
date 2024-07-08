# Define Makefile

PYTHON = python3

# Default target
.PHONY: run
# run is a phony target
# Does not corresopond to a file, but is a command.

# Command to install packages from requirements:
run:
	@echo "Installing packges from requirements.txt........"
	@$(PYTHON) -m pip install -r requirements.txt
	@echo "Installation complete!"