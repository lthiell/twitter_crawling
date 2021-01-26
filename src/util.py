def read_txt(filename):
    f = open(filename, "r")
    return list(filter(lambda e: e != "", f.read().split("\n")))