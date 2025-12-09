import importlib.metadata


def get_version_from_installed(package_name):
    return importlib.metadata.version(package_name)
