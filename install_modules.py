import importlib
from install_modules import *

# List of required libraries
required_libraries = ['pyodbc', 'flask', 're', 'pymysql', 'brickschema', 'pandas', 'sqlalchemy']

# Function to check if a library is installed
def is_library_installed(library_name):
    try:
        importlib.import_module(library_name)
        return True
    except ImportError:
        return False

# Install missing libraries
missing_libraries = [lib for lib in required_libraries if not is_library_installed(lib)]

def install_missing_libraries(missing_libraries):
    if missing_libraries:
        import subprocess
        for lib in missing_libraries:
            subprocess.call(['pip', 'install', lib])
        print("Installed missing libraries:", missing_libraries)
    else:
        print("All required libraries are already installed.")