from ror_matcher.models import hash_affiliation


def test_hash_produces_16_char_hex():
    h = hash_affiliation("University of Oxford")
    assert len(h) == 16
    assert all(c in "0123456789abcdef" for c in h)


def test_hash_is_deterministic():
    assert hash_affiliation("MIT") == hash_affiliation("MIT")


def test_hash_differs_for_different_inputs():
    assert hash_affiliation("MIT") != hash_affiliation("Stanford University")


def test_hash_handles_unicode():
    h = hash_affiliation("Universit\u00e9 de Paris")
    assert len(h) == 16


def test_hash_handles_empty_string():
    h = hash_affiliation("")
    assert len(h) == 16
