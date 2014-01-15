# Build instructions

## Installing zookeeper on OSX

Using homebrew (--c installs C headers):

    brew install zookeeper --c

## Installing dependencies

Install dependencies including zookeeper bindings from:
https://github.com/motus/haskell-zookeeper-client . Remember to pass `--extra-include-dirs=/usr/local/Cellar/zookeeper/ZOOKEEPER_VERSION/include/zookeeper/` to `cabal install` when building haskell-zookeeper-client.

Then:

    cabal configure
    cabal build

# Initial setup

## Configuring zookeeper

Here is a basic local zk config: https://gist.github.com/DanielWaterworth/6ab8d009e6d8e7bfa600

## Stronghold initial setup

```
$ ./dist/build/wipe/wipe <zk address>
```

# Invocation

```
stronghold <port> <zk address>
```
