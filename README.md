# Event Source Atom Data

This project processes events from the [Oracle Event Store](https://github.com/xtracdev/oraeventstore),
writing them into organized storage to allow navigating published events
using the atom protocol.

This project uses two tables: a recent table, and an archive table. Recent 
events go in the recent table, archived go in the archived table.

As events get written to the recent table, once the size threshold for a feed is read,
they are assigned a feed id and moved to the archive table. An additional table, 
feeds, is used to keep track of the relationship between feeds, so we can
identity the previous feed from recent, and can navigate backwards and 
forwards through the event history using the link relations.

## Table Definitions

<pre>
create table recent (
    id number generated always as identity,
    feedid varchar2(100),
    event_time timestamp DEFAULT current_timestamp,
    aggregate_id varchar2(60)not null,
    version integer not null,
    typecode varchar2(30) not null,
    payload blob
);

create table archive (
    feedid varchar2(100),
    event_time timestamp DEFAULT current_timestamp,
    aggregate_id varchar2(60)not null,
    version integer not null,
    typecode varchar2(30) not null,
    payload blob,
    primary key (aggregate_id, feedid)
);

create table feeds (
    id  number generated always as identity,
    event_time timestamp DEFAULT current_timestamp,
    feedid varchar2(100) not null,
    previous varchar2(100)
);
</pre>

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