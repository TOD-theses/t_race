from pytest import FixtureRequest, fixture
from pathlib import Path
import sys


@fixture
def root_dir(request: FixtureRequest) -> Path:
    return request.config.rootpath


# each test runs on cwd to its temp dir
@fixture(autouse=True)
def go_to_tmpdir(request: FixtureRequest):
    # Get the fixture dynamically by its name.
    tmpdir = request.getfixturevalue("tmpdir")
    # ensure local test created packages can be imported
    sys.path.insert(0, str(tmpdir))
    # Chdir only for the duration of the test.
    with tmpdir.as_cwd():
        yield
