import pathlib
from os import makedirs

def get_all_subclasses(cls):
    all_subclasses = []

    for subclass in cls.__subclasses__():
        all_subclasses.append(subclass)
        all_subclasses.extend(get_all_subclasses(subclass))

    return all_subclasses


def create_dirs(*dir_paths: pathlib) -> None:
    for path in dir_paths:
        makedirs(path, exist_ok=True)


def get_module_dir():
    return pathlib.Path(__file__).parent.absolute()

