# Build instructions

Install dependencies including zookeeper bindings from: 
https://github.com/motus/haskell-zookeeper-client . Then:

    cabal configure
    cabal build

# Initial setup

## Configuring zookeeper

Here is a basic local zk config: https://gist.github.com/DanielWaterworth/6ab8d009e6d8e7bfa600

## Stronghold initial setup

```
$ zkCLI
[zk: localhost:2181(CONNECTED) 1] create /ref ""
[zk: localhost:2181(CONNECTED) 2] create /head ""
[zk: localhost:2181(CONNECTED) 3] quit
$ ./dist/build/wipe/wipe
```

# Invocation

```
stronghold <port> <zk address>
```
