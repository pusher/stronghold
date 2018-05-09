# Stronghold, an alternative to etcd

Stronghold is a global distributed key-value store for keeping application configuration.
It's a bit like etcd, or the KV module of consul.

Stronghold is client-server.
The server is [Apache ZooKeeper](https://zookeeper.apache.org/).
The main client is `stronghold`, which provides an HTTP API to query ZooKeeper.
That is what this repository provides.
`stronghold` is designed to run as a local daemon on all hosts in your system,
and should be the only way you interact with ZooKeeper.
For another layer of indirection,
see other client libraries which use the `stronghold` daemon:

* A web GUI client, [`stronghold-ui`](https://github.com/pusher/stronghold-ui).
  This queries the `stronghold` HTTP server.
* A CLI client, [`stronghold-cli`](https://github.com/pusher/stronghold-ruby).
  This also queries the `stronghold` HTTP server.

## Data model

Stronghold features:

* Hierarchical configuration, so global defaults may be set and
  overridden at any of the cluster, machine type, individual machine
  and individual process levels.
  Each state in the history is a tree of JSON objects, where the edges are
  labelled.
* A complete history of changes, so you can find out what
  configuration was being used in a particular process at a particular
  point in time.
  The way to think about history in stronghold is as a singly linked list.
  Each item is a state and is labelled by a sha1 hash. The links in the
  list are changes, they are unlabelled. You can only reference them by
  there proximity to states.
* Attribution of changes to users, so you can ask someone why they
  changed something if the comment isn't clear enough!

## API

Stronghold has an HTTP API with the following resources:

### `GET /head`

Fetches the current version, returned in the body.

### `GET /versions?at=[ts]`

Fetches the revision active at ts.

### `GET /versions?last=[last]&size=[size]`

Fetches information on up to size changes before last (a revision).
Returns a JSON array containing one object per change in chronological
order. Each has at least the following keys:

* comment - the comment given,
* author - the author's name,
* timestamp - the timestamp of the change, UTC seconds,
* paths - the paths within the hierarchy that were updated,
* revision - the first revision that reflects the update
* size is optional

### GET /versions?first=[first]&limit=[limit]&size=[size]

Fetches information on changes that occur after first and ending at
either limit (a revision) or after size changes. The response is in the
same form as the previous method. `size` is optional.

### GET /[version]/tree/paths

Returns a JSON list of paths in the hierarchy at version.

### GET /[version]/tree/materialized/[path]

Fetches the materialized JSON for a particular path at version.

### GET /[version]/next/tree/materialized/[path]

Blocks until the materialized JSON for a particular path changes and
returns a JSON object containing:

* data - the new materialized JSON,
* revision - the corresponding revision.

### GET /[version]/tree/peculiar/[path]

Fetches the JSON peculiar to a particular path.

### GET /[version]/change

Fetches information on the change immediately before version. Returns a
JSON object containing:

* comment,
* author,
* timestamp - UTC seconds,
* changes - A JSON list containing objects describing every path that
  changed. Each object contains:
  * path - the path in question,
  * old - the old JSON,
  * new - the updated JSON.

### POST /[version]/update/[path]

Method used to make an update. The body should contain a JSON object
with the following keys:

* author - the name of the author,
* comment - a short comment describing the change,
* data - the new json object to put at path.

# Build instructions

NB: There is a development mode which is backed by sqlite. This is likely to be
simpler. You must still install the zookeeper dependency in order for it to
compile.

## Installing dependencies (on OSX)

```sh
brew install zookeeper
export CPATH=$(brew --prefix zookeeper)/include:$CPATH
```

## Installing dependencies on debian / ubuntu

```sh
sudo apt-get install libzookeeper-mt-dev
```

## Building Stronghold

```sh
stack build
```

# Initial setup

## Configuring zookeeper

Here is a basic local zk config: https://gist.github.com/DanielWaterworth/6ab8d009e6d8e7bfa600

## Stronghold initial setup

To initialize the state in zookeeper.

```sh
stack exec wipe -- localhost:2181
```

# Invocation

```sh
stronghold <port> <zk address>
```

Running locally, you'll probably want:

```sh
stack exec stronghold -- 5040 localhost:2181
```

or

```sh
foreman start
```

There's also a development mode that uses SQLite for storage instead of zookeeper:

```sh
stack exec stronghold -- development 5040 ./data.db
```
