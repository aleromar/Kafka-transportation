import os

def get_version():
    thisfilepath = os.path.dirname(os.path.realpath(__file__))
    filepath = os.path.join(thisfilepath, "..","..","application_version.txt")
    with open(filepath,'r') as f:
        version_number = f.read()
    return int(version_number)