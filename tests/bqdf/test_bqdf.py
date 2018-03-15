#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `bqdf` package."""

import pytest
from tests.conftest import *


@fixture
def sample_fixture():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    return 1


def test_something(sample_fixture):
    assert 1 == sample_fixture
