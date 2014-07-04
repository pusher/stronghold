# Build instructions

## Installing zookeeper on OSX

NB: There is a development mode which is backed by sqlite. Depending on what you're planning, this might well be suitable and much simpler.

Using homebrew (--c installs C headers):

    # Install Java using Brew cask (or got to http://java.com/en/download/ )
    brew tap phinze/cask
    brew install brew-cask
    brew cask install java

    # Install zookeeper (with the C include headers)
    brew install zookeeper --c

## Installing Haskell

    brew install haskell-platform # Time for a coffee..
    # haskell-platform 2013.2.0.0 has an old cabal which doesn't have the `cabal sandbox` command
    cabal update
    cabal install cabal-install
    # .. and add this to your .profile or use direnv
    export PATH=~/.cabal/bin:$PATH

## Building Stronghold

    # In this directory
    cabal sandbox init

    # Somewhere else
    git clone git@github.com:motus/haskell-zookeeper-client.git

    # Back in this directory
    cabal sandbox add-source path/to/haskell-zookeeper-client
    cabal install zookeeper --extra-include-dirs=`brew --prefix zookeeper`/include/zookeeper/ # or wherever your zookeeper headers are
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

    cabal run stronghold -- 5040 localhost:2181

    or

    foreman start

There's also a development mode that uses SQLite for storage instead of zookeeper:

    cabal run stronghold -- development 5040 ./data.db
