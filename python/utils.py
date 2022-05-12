import os

def get_key(file):
    try:
        with open(file, "rb") as file:
            key = file.readline()
    except IOError:
        return None

    return key

def write_key(file, key):
    try:
        with open(file, "w") as file:
            file.write(key)
    except IOError:
        return None
