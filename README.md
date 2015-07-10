# Build instructions

## Installing dependencies (on OSX)

NB: There is a development mode which is backed by sqlite. Depending on what
you're planning, this might well be suitable and much simpler.

Using homebrew:

```sh
# Install zookeeper
brew install zookeeper
```

## Building Stronghold

```sh
export CPATH=$(brew --prefix zookeeper)/include/zookeeper:$CPATH
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
