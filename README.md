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
    timestamp DEFAULT current_timestamp,
    feedid varchar2(100) not null,
    previous varchar2(100)
);
</pre>

