module ZkInterface (
  ZkInterface,
  newZkInterface,
  getHead,
  getHeadBlockIfEq,
  loadData,
  storeData,
  hasReference,
  updateHead
) where

{-
  This module is a higher level interface for zookeeper, exposing only the 
  operations that are pertinent to us.
-}

import Data.Maybe (isJust, maybe)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Base16 as Base16

import Control.Applicative ((<$>), (<*>), empty)
import Control.Monad (guard, join)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Maybe (MaybeT(MaybeT), runMaybeT)
import Control.Exception (tryJust, Exception)
import Control.Concurrent.STM (STM, atomically, retry, TVar, readTVar, writeTVar, newTVarIO, readTVarIO)

import Crypto.Hash.SHA1 (hash)

import qualified Zookeeper as Zoo

import LRUCache (newLRU)

data ZkInterface = ZkInterface (ByteString -> IO ByteString) !(TVar (Maybe ByteString)) !Zoo.ZHandle

tryGetHeadSTM :: ZkInterface -> STM (Maybe ByteString)
tryGetHeadSTM (ZkInterface _ head _) = readTVar head

getHeadSTM :: ZkInterface -> STM ByteString
getHeadSTM zk = tryGetHeadSTM zk >>= maybe retry return

getHead :: ZkInterface -> MaybeT IO B.ByteString
getHead (ZkInterface _ head _) = MaybeT (readTVarIO head)

getHeadBlockIfEq :: ZkInterface -> B.ByteString -> MaybeT IO B.ByteString
getHeadBlockIfEq zk ref = lift $ atomically $ do
  head <- getHeadSTM zk
  if head == ref then do
    retry
   else
    return head

fetchHeadAndWatch :: ZkInterface -> IO ()
fetchHeadAndWatch (ZkInterface _ head zk) = do
  (dat, _) <- Zoo.get zk "/head" Zoo.Watch
  atomically $ writeTVar head dat

watcher :: ZkInterface -> Zoo.ZHandle -> Zoo.EventType -> Zoo.State -> String -> IO ()
watcher zk _ Zoo.Changed _ "/head" =
  fetchHeadAndWatch zk
watcher zk _ Zoo.Session Zoo.Connected _ =
  fetchHeadAndWatch zk
watcher _ _ zEventType zState path =
  putStrLn ("watch: '" ++ path ++ "' :: " ++ show zEventType ++ " " ++ show zState)

newZkInterface :: String -> IO ZkInterface
newZkInterface hostPort = do
  zk <- Zoo.init hostPort Nothing 10000
  fetchNode' <- newLRU (fetchNode zk) 10000
  interface <- ZkInterface fetchNode' <$> newTVarIO Nothing <*> return zk
  Zoo.setWatcher zk (Just (watcher interface))
  return interface
 where
  fetchNode :: Zoo.ZHandle -> ByteString -> IO ByteString
  fetchNode zk ref =
    fst <$> Zoo.get zk (getZkPath ref) Zoo.NoWatch >>= maybe (fail "no such node") return

getZkPath :: B.ByteString -> String
getZkPath = ("/ref/" ++) . BC.unpack

isErrNodeExists :: Zoo.ZooError -> Bool
isErrNodeExists (Zoo.ErrNodeExists _) = True
isErrNodeExists _ = False

isErrNoNode :: Zoo.ZooError -> Bool
isErrNoNode (Zoo.ErrNoNode _) = True
isErrNoNode _ = False

isConnectionError :: Zoo.ZooError -> Bool
isConnectionError (Zoo.ErrConnectionLoss _) = True
isConnectionError _ = False

orFn :: (a -> Bool) -> (a -> Bool) -> (a -> Bool)
orFn f g x = f x || g x

tryMaybeT :: Exception e => (e -> Bool) -> IO a -> MaybeT IO a
tryMaybeT f a = lift (tryJust (guard . f) a) >>= either (const empty) (return)

loadData :: ZkInterface -> B.ByteString -> MaybeT IO B.ByteString
loadData (ZkInterface fetchNode _ zk) ref =
  -- fail on: no node, connection error
  -- TODO: handle the no data case better
  tryMaybeT (isErrNoNode `orFn` isConnectionError) (fetchNode ref)

storeData :: ZkInterface -> B.ByteString -> MaybeT IO B.ByteString
storeData (ZkInterface _ _ zk) d = do
  let h = (Base16.encode . hash) d
  -- fail on connection errors, but ignore node exists errors
  tryMaybeT isConnectionError $ tryJust (guard . isErrNodeExists) $
    Zoo.create zk (getZkPath h) (Just d) Zoo.OpenAclUnsafe (Zoo.CreateMode False False)
  return h

hasReference :: ZkInterface -> B.ByteString -> MaybeT IO Bool
hasReference (ZkInterface _ _ zk) ref =
  (fmap (either (const False) (isJust . fst)) .
   tryMaybeT isConnectionError .
   tryJust (guard . isErrNoNode)) (Zoo.get zk (getZkPath ref) Zoo.NoWatch)

updateHead :: ZkInterface -> B.ByteString -> B.ByteString -> MaybeT IO Bool
updateHead (ZkInterface _ _ zk) old new = do
  (dat, stat) <- tryMaybeT isConnectionError $ Zoo.get zk "/head" Zoo.NoWatch
  dat' <- MaybeT (return dat)
  if old == dat' then do
    result <- tryMaybeT isConnectionError $
      Zoo.set zk "/head" (Just new) (fromIntegral (Zoo.stat_version stat))
    return True
   else
    return False
