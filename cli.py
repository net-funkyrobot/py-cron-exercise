from argparse import ArgumentTypeError
from typing import Callable, Iterable
from datetime import datetime, timedelta
from functools import partial, update_wrapper
from milc import cli
import re
from rx import from_iterable, operators as op
from rx.core.typing import Observable
from sys import stdin

CRON_ENTRY_REGEX = r"^(\d{1,2}|\*)\s(\d{1,2}|\*)\s(.+)$"


def _parse_cron(entry: str) -> tuple[str, str, str]:
    """Parses and validates a simplified cron entry into a tuple with the
    following format:
    * * /path/to/script
    0 0 /path/to/script
    59 23 /path/to/script"""

    def validate(limit, value):
        return value == "*" or int(value) <= limit

    match_obj = re.match(CRON_ENTRY_REGEX, entry)
    if not match_obj:
        raise ValueError(f"Invalid cron entry: {entry}")

    m, h, scr = match_obj.group(1), match_obj.group(2), match_obj.group(3)
    if not validate(59, m) or not validate(23, h):
        raise ValueError(f"Invalid cron time for entry {entry}")

    return m, h, scr


def _create_mapper(
    now: datetime,
) -> Callable[[tuple[str, str, str]], tuple[str, str, str]]:
    """Creates the core mapper that maps the cron entry into the expected
    output values"""
    dt = update_wrapper(
        partial(datetime, now.year, now.month, now.day),
        datetime,
    )

    def calc_next_time(m: str, h: str) -> datetime:
        """Calculates the datetime the entry will next run"""
        if (m, h) == ("*", "*"):
            # Next minunte
            return now + timedelta(minutes=1)
        elif h == "*":
            # Every hour at m minutes
            minutes = int(m)
            next_dt = dt(now.hour) + timedelta(minutes=minutes)
            if next_dt <= now:
                next_dt = next_dt + timedelta(hours=1)
            return next_dt
        elif m == "*":
            # Every minute during h hour
            hours = int(h)
            if now.hour == hours:
                # Currently inside specified hour, return next minute
                return dt(now.hour, now.minute) + timedelta(minutes=1)
            elif now.hour < hours:
                # Specified hour later today
                return datetime(
                    now.year,
                    now.month,
                    now.day,
                    hours,
                    0,
                )
            else:
                # Specified hour next occurs tomorrow
                return (
                    datetime(
                        now.year,
                        now.month,
                        now.day,
                        hours,
                        0,
                    )
                    + timedelta(days=1)
                )

        else:
            # Once daily at specified hour and minute
            hours, minutes = int(h), int(m)
            next_dt = dt(hours, minutes)
            if next_dt <= now:
                next_dt = (
                    datetime(
                        now.year,
                        now.month,
                        now.day,
                        hours,
                        minutes,
                    )
                    + timedelta(days=1)
                )
            return next_dt

    def map_entry(entry: tuple[str, str, str]) -> tuple[str, str, str]:
        """Mapping function"""
        m, h, str = entry
        next_dt = calc_next_time(m, h)

        return (
            next_dt.strftime("%H:%M"),
            "tomorrow" if next_dt.day > now.day else "today",
            str,
        )

    return map_entry


def _valid_datetime(value: str) -> datetime:
    """Tests for a valid time and returns the given time on today's date"""
    try:
        time = datetime.strptime(value, "%H:%M")
        today = datetime.now()

        return datetime(
            today.year,
            today.month,
            today.day,
            time.hour,
            time.minute,
        )
    except ValueError as e:
        raise ArgumentTypeError(e)


def _create_obserable(stream: Iterable, now: datetime) -> Observable:
    return from_iterable(stream).pipe(
        op.map(_parse_cron),
        op.map(_create_mapper(now)),
        op.map(lambda v: str.join(" ", v)),
    )


@cli.argument("-t", "--time", default=datetime.now(), type=_valid_datetime)
@cli.entrypoint(
    "Parse a simplified crontab and output next times for each entry.",
)
def main(cli):
    now = cli.config.general.time

    cli.log.info("Parsing stdin")

    _create_obserable(stdin, now).subscribe(
        cli.echo,
        cli.log.exception,
        lambda: cli.log.info("Done."),
    )


if __name__ == "__main__":
    cli()
