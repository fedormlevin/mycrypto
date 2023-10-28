#!/usr/bin/env python

import os
from datetime import datetime

def main():
    # Create the directory if it doesn't exist
    dir_name = "datetime_directory"
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    # Append the current datetime to test.txt
    with open(os.path.join(dir_name, "test.txt"), "a") as file:
        file.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n')

if __name__ == "__main__":
    main()
