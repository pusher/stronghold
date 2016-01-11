{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}
module Main where

import Control.Exception
import Crypto.Hash.SHA1 (hash)
import Data.ByteString.Char8
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import System.Environment (getArgs)

import qualified Data.ByteString.Base16 as Base16
import qualified Zookeeper as Zoo

nilNode = "3n"

nilHash = Base16.encode (hash nilNode)

attemptZK :: IO a -> IO ()
attemptZK act =
  tryZK act >> return ()
 where
  tryZK :: IO a -> IO (Either Zoo.ZooError a)
  tryZK = try

main :: IO ()
main = do
  [portString] <- getArgs
  zk <- Zoo.init portString Nothing 10000

  attemptZK $ do
    children <- Zoo.getChildren zk "/ref" Zoo.NoWatch
    mapM_ (\child -> Zoo.delete zk ("/ref/" ++ child) (-1)) children
  attemptZK $ Zoo.delete zk "/ref" (-1)

  attemptZK $ Zoo.create zk "/ref" Nothing Zoo.OpenAclUnsafe (Zoo.CreateMode False False)
  attemptZK $ Zoo.create zk "/head" Nothing Zoo.OpenAclUnsafe (Zoo.CreateMode False False)
  attemptZK $ Zoo.create zk ("/ref/" ++ unpack nilHash) (Just nilNode) Zoo.OpenAclUnsafe (Zoo.CreateMode False False)
  attemptZK $ Zoo.set zk "/head" (Just nilHash) (-1)
