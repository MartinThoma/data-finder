# First party modules
from data_finder.spider import is_valid


def test_is_valid():
    assert is_valid("https://martin-thoma.com/")
    assert is_valid("http://martin-thoma.com/")
    assert is_valid("http://martin-thoma.com")
    assert not is_valid("martin-thoma.com/")
    assert not is_valid("http://")
    assert is_valid("http://martin-thoma")
    assert is_valid("http://martin-thoma.")
    assert not is_valid("")
    assert not is_valid("foo.bar")
