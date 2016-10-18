# Event Source Atom Data

[![CircleCI](https://circleci.com/gh/xtracdev/es-atom-data.svg?style=svg)](https://circleci.com/gh/xtracdev/es-atom-data)

This project processes events from the [Oracle Event Store](https://github.com/xtracdev/oraeventstore),
writing them into organized storage to allow navigating published events
using the atom protocol.

This project uses two tables: the atom_event table to store the events
associated with the atom feed, and the feed table, which keeps track of the 
feeds. The overall event history can be traversed from the current feed
backwards using the previous entry in the feeds table. The recent items
are those that have not been assigned a feedid.

As events get written to the recent table, once the size threshold for a feed is read,
they are assigned a feed id. The default page size is 100 items; this may be
overridden using the FEED_THRESHOLD environment variable.

## Table Definitions

<pre>
create table atom_event (
    id number generated always as identity,
    feedid varchar2(100),
    event_time timestamp DEFAULT current_timestamp,
    aggregate_id varchar2(60)not null,
    version integer not null,
    typecode varchar2(30) not null,
    payload blob
);

create index atom_event_feedid_ix on atom_event (feedid);

create table feed (
    id  number generated always as identity,
    event_time timestamp DEFAULT current_timestamp,
    feedid varchar2(100) not null,
    previous varchar2(100)
);

create index feed_feedid_ix on feed(feedid);
create index feed_previous_ix on feed(previous);
</pre>

## Testing

This package has unit tests that may be run using go test, and integration
tests that may be run using gucumber. Note the integration tests requires
a live instance of Oracle.

*Caution* The integration tests delete all records from the event and 
feed table to run in a know good state.

## Viewing Emitted Statsd Telemetry Data

This package emits counters and timing data via statsd, using the
[go-metrics](https://github.com/armon/go-metrics) library. If the
STATSD_ENDPOINT environment variable is set, a statsd sink is 
configured, otherwise the in memory accumulator is used.

A quick and dirty way to do this if you don't have sumo or some other way to look
at telemetry data is to fireup nc in another window, and configure the 
process to write to that endpoint via the STATSD_ENDPOINT environment 
variable.

For example if you have this listener:

<pre>
nc -u -l localhost 57719
</pre>

And you export STATSD_ENDPOINT thusly:

<pre>
export STATSD_ENDPOINT=localhost:57719
</pre>

When you run the gucumber tests you will set the output.

## Contributing

To contribute, you must certify you agree with the [Developer Certificate of Origin](http://developercertificate.org/)
by signing your commits via `git -s`. To create a signature, configure your user name and email address in git.
Sign with your real name, do not use pseudonyms or submit anonymous commits.


In terms of workflow:

0. For significant changes or improvement, create an issue before commencing work.
1. Fork the respository, and create a branch for your edits.
2. Add tests that cover your changes, unit tests for smaller changes, acceptance test
for more significant functionality.
3. Run gofmt on each file you change before committing your changes.
4. Run golint on each file you change before committing your changes.
5. Make sure all the tests pass before committing your changes.
6. Commit your changes and issue a pull request.

## License

(c) 2016 Fidelity Investments
Licensed under the Apache License, Version 2.0MACLB015803:es-atom-data a045103$ 