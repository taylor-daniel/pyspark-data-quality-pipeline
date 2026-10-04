import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.pipeline import get_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = get_spark("tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
