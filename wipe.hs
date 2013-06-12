{-# LANGUAGE OverloadedStrings #-}
module Main where

import Data.ByteString.Char8
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import qualified Data.ByteString.Base16 as Base16

import Crypto.Hash.SHA1

import qualified Zookeeper as Zoo

nilNode = "3n"

nilHash = Base16.encode (hash nilNode)

main :: IO ()
main = do
  zk <- Zoo.init "localhost:2181" Nothing 10000
  children <- Zoo.getChildren zk "/ref" Zoo.NoWatch
  mapM_ (\child -> Zoo.delete zk ("/ref/" ++ child) (-1)) children
  Zoo.create zk ("/ref/" ++ unpack nilHash) (Just nilNode) Zoo.OpenAclUnsafe (Zoo.CreateMode False False)
  Zoo.set zk "/head" (Just nilHash) (-1)
