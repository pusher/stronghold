# Build instructions

## Installing dependencies (on OSX)

NB: There is a development mode which is backed by sqlite. Depending on what you're planning, this might well be suitable and much simpler.

Using homebrew:

    # Install zookeeper
    brew install zookeeper

    # Install Haskell
    brew install ghc cabal-install

## Building Stronghold

    # In this directory
    cabal sandbox init

    git submodule update --init

    cabal sandbox add-source vendor/zookeeper
    cabal install zookeeper --extra-include-dirs=`brew --prefix zookeeper`/include/zookeeper/ # or wherever your zookeeper headers are
    cabal install --only-dependencies # This will take some time
    cabal configure
    cabal build

# Initial setup

## Configuring zookeeper

Here is a basic local zk config: https://gist.github.com/DanielWaterworth/6ab8d009e6d8e7bfa600

## Stronghold initial setup

To initialize the state in zookeeper.

    $ ./dist/build/wipe/wipe localhost:2181

# Invocation

    stronghold <port> <zk address>

Running locally, you'll probably want:

    cabal run stronghold -- 5040 localhost:2181

    or

    foreman start

There's also a development mode that uses SQLite for storage instead of zookeeper:

    cabal run stronghold -- development 5040 ./data.db
