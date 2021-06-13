from argparse import ArgumentTypeError
import pytest
from datetime import datetime
from functools import partial, update_wrapper

from cli import _valid_datetime, _parse_cron, _create_mapper

POS = (
    ("30", "1", "/bin/run_me_daily", "01:30", "tomorrow"),
    ("45", "*", "/bin/run_me_hourly", "16:45", "today"),
    ("*", "*", "/bin/run_me_every_minute", "16:10", "today"),
    ("*", "19", "/bin/run_me_sixty_times", "19:00", "today"),
    ("59", "23", "/path/to/script", "23:59", "today"),
    ("0", "0", "/path/to/script", "00:00", "tomorrow"),
)


NOW = datetime.now()
dt = update_wrapper(
    partial(datetime, NOW.year, NOW.month, NOW.day),
    datetime,
)


def test_mapper():
    mapper = _create_mapper(dt(16, 9))

    for ex in POS:
        example, expected = ex[:3], ex[3:]
        out = mapper(example)
        assert out == (*expected, example[2])


def test_parse_cron():
    pos = map(lambda ex: " ".join(ex[:3]), POS)
    for i, example in enumerate(pos):
        tup = _parse_cron(example)
        assert len(tup) == 3
        assert tup[0] == POS[i][0] and tup[1] == POS[i][1] and tup[2] == POS[i][2]

    print("Negative examples:")
    neg = (
        "60 1 /path/to/script",
        "59 24 /path/to/script",
        "60 - /path/to/script",
        "59 /path/to/script",
    )
    for example in neg:
        print(example)
        with pytest.raises(ValueError):
            _parse_cron(example)


def test_valid_datetime():
    pos = (
        ("16:10", dt(16, 10)),
        ("00:00", dt(0, 0)),
        ("23:59", dt(23, 59)),
        ("9:30", dt(9, 30)),
        ("09:3", dt(9, 3)),
    )
    neg = (
        "14",
        "14:",
        ":30",
        "1430",
        "930",
        "time",
        "",
    )

    print("Positive examples:")
    for example, expected in pos:
        d = _valid_datetime(example)
        assert d == expected

    print("Negative examples:")
    for example in neg:
        print(example)
        with pytest.raises(ArgumentTypeError):
            _valid_datetime(example)

    with pytest.raises(TypeError):
        _valid_datetime(None)
