module ZkInterface where

{-
  This module is a higher level interface for zookeeper, exposing only the 
  operations that are pertinent to us.
-}

import Data.Maybe (isJust, maybe)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Base16 as Base16

import Control.Applicative ((<$>), (<*>))
import Control.Monad (guard, join)
import Control.Exception (tryJust)
import Control.Concurrent.STM (STM, atomically, retry, TVar, readTVar, writeTVar, newTVarIO, readTVarIO)

import Crypto.Hash.SHA1 (hash)

import qualified Zookeeper as Zoo

data ZkInterface = ZkInterface !(TVar (Maybe ByteString)) !Zoo.ZHandle

tryGetHeadSTM :: ZkInterface -> STM (Maybe ByteString)
tryGetHeadSTM (ZkInterface head _) = readTVar head

getHeadIfNot :: ZkInterface -> ByteString -> IO ByteString
getHeadIfNot zk ref = atomically $ do
  head <- getHeadSTM zk
  if head == ref then do
    retry
   else
    return head

getHeadSTM :: ZkInterface -> STM ByteString
getHeadSTM zk = tryGetHeadSTM zk >>= maybe retry return

getHead :: ZkInterface -> IO (Maybe B.ByteString)
getHead (ZkInterface head _) = readTVarIO head

fetchHeadAndWatch :: ZkInterface -> IO ()
fetchHeadAndWatch (ZkInterface head zk) = do
  (dat, _) <- Zoo.get zk "/head" Zoo.Watch
  atomically $ writeTVar head dat

watcher :: ZkInterface -> Zoo.ZHandle -> Zoo.EventType -> Zoo.State -> String -> IO ()
watcher zk _ Zoo.Changed _ "/head" = do
  print "head update"
  fetchHeadAndWatch zk
watcher _ _ zEventType zState path =
  putStrLn ("watch: '" ++ path ++ "' :: " ++ show zEventType ++ " " ++ show zState)

newZkInterface :: String -> IO ZkInterface
newZkInterface hostPort = do
  zk <- Zoo.init hostPort Nothing 10000
  interface <- ZkInterface <$> newTVarIO Nothing <*> return zk
  Zoo.setWatcher zk (Just (watcher interface))
  fetchHeadAndWatch interface
  return interface

getZkPath :: B.ByteString -> String
getZkPath = ("/ref/" ++) . BC.unpack

isErrNodeExists :: Zoo.ZooError -> Bool
isErrNodeExists (Zoo.ErrNodeExists _) = True
isErrNodeExists _ = False

isErrNoNode :: Zoo.ZooError -> Bool
isErrNoNode (Zoo.ErrNoNode _) = True
isErrNoNode _ = False

-- TODO: handle disconnected zookeeper
loadData :: ZkInterface -> B.ByteString -> IO (Maybe B.ByteString)
loadData (ZkInterface _ zk) ref = do
  result <- tryJust (guard . isErrNoNode) (Zoo.get zk (getZkPath ref) Zoo.NoWatch)
  return (either (const Nothing) fst result)

storeData :: ZkInterface -> B.ByteString -> IO (Maybe B.ByteString)
storeData (ZkInterface _ zk) d = do
  let h = (Base16.encode . hash) d
  tryJust (guard . isErrNodeExists) $
    Zoo.create zk (getZkPath h) (Just d) Zoo.OpenAclUnsafe (Zoo.CreateMode False False)
  return (Just h)

hasReference :: ZkInterface -> B.ByteString -> IO Bool
hasReference zk r = isJust <$> loadData zk r

updateHead :: ZkInterface -> B.ByteString -> B.ByteString -> IO Bool
updateHead (ZkInterface _ zk) old new = do
  (dat, stat) <- Zoo.get zk "/head" Zoo.NoWatch
  dat' <- maybe (fail "no head") return dat
  if old == dat' then do
    -- FIXME: exceptions here should be caught
    result <- Zoo.set zk "/head" (Just new) (fromIntegral (Zoo.stat_version stat))
    return True
   else
    return False
