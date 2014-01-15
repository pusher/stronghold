# Build instructions

## Installing zookeeper on OSX

Using homebrew (--c installs C headers):

    brew install zookeeper --c

## Building Stronghold

    # In this directory
    cabal sandbox init

    # Somewhere else
    git clone git@github.com:motus/haskell-zookeeper-client.git

    # Back in this directory
    cabal sandbox add-source path/to/haskell-zookeeper-client
    cabal install zookeeper --extra-include-dirs=/usr/local/Cellar/zookeeper/ZOOKEEPER_VERSION/include/zookeeper/ # or wherever your zookeeper headers are
    cabal install --only-dependencies # This will take some time
    cabal configure
    cabal build

# Initial setup

## Configuring zookeeper

Here is a basic local zk config: https://gist.github.com/DanielWaterworth/6ab8d009e6d8e7bfa600

## Stronghold initial setup

To initialize the state in zookeeper.

    $ ./dist/build/wipe/wipe <zk address>

# Invocation

    stronghold <port> <zk address>

Running locally, you'll probably want:

    ./dist/build/stronghold/stronghold 5040 localhost:2181
