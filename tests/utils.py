from ecc.utils import load_toml


def load_test_toml():
    return load_toml('edgedbcloud.toml', 'test-edgedb-cloud')
