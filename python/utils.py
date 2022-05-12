"""
This work is licensed under the terms of the MIT license.  
For a copy, see <https://opensource.org/licenses/MIT>.

Developed by NISLAB - Network and Information Security Laboratory
at George Emil Palade University of Medicine, Pharmacy, Science and
Technology of Târgu Mureş <https://nislab.umfst.ro/>

Contributors: Roland Bolboaca
"""


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
