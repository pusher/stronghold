module ZkInterface where

{-
  This module is a higher level interface for zookeeper, exposing only the 
  operations that are pertinent to us.
-}

import Data.Maybe (isJust)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Base16 as Base16

import Control.Applicative ((<$>))
import Control.Monad (guard, join)
import Control.Exception (tryJust)

import Crypto.Hash.SHA1 (hash)

import qualified Zookeeper as Zoo

data ZkInterface = ZkInterface !Zoo.ZHandle

watcher :: ZkInterface -> Zoo.ZHandle -> Zoo.EventType -> Zoo.State -> String -> IO ()
watcher _ _ Zoo.Changed _ "/head" = print "head reference change"
watcher _ _ zEventType zState path =
  putStrLn ("watch: '" ++ path ++ "' :: " ++ show zEventType ++ " " ++ show zState)

newZkInterface :: String -> IO ZkInterface
newZkInterface hostPort = do
  zk <- Zoo.init hostPort Nothing 10000
  let interface = ZkInterface zk
  Zoo.setWatcher zk (Just (watcher interface))
  return interface

getZkPath :: B.ByteString -> String
getZkPath = ("/ref/" ++) . BC.unpack . Base16.encode

isErrNodeExists :: Zoo.ZooError -> Bool
isErrNodeExists (Zoo.ErrNodeExists _) = True
isErrNodeExists _ = False

isErrNoNode :: Zoo.ZooError -> Bool
isErrNoNode (Zoo.ErrNoNode _) = True
isErrNoNode _ = False

-- TODO: handle disconnected zookeeper
loadData :: ZkInterface -> B.ByteString -> IO (Maybe B.ByteString)
loadData (ZkInterface zk) ref = do
  result <- tryJust (guard . isErrNoNode) (Zoo.get zk (getZkPath ref) Zoo.NoWatch)
  return (either (const Nothing) fst result)

isRight :: Either a b -> Bool
isRight (Right _) = True
isRight _ = False

storeData :: ZkInterface -> B.ByteString -> IO (Maybe B.ByteString)
storeData (ZkInterface zk) d = do
  let h = hash d
  succeed <- isRight <$> (tryJust (guard . isErrNodeExists) $
    Zoo.create zk (getZkPath h) (Just d) Zoo.OpenAclUnsafe (Zoo.CreateMode False False))
  if succeed then
    return (Just h)
   else
    return Nothing

hasReference :: ZkInterface -> B.ByteString -> IO Bool
hasReference zk r =
  let (r', _) = Base16.decode r in
    if B.null r' then
      return False
     else
      isJust <$> loadData zk r

updateHead :: ZkInterface -> B.ByteString -> B.ByteString -> IO Bool
updateHead (ZkInterface zk) old new = do
  (dat, stat) <- Zoo.get zk "/head" Zoo.NoWatch
  dat' <- maybe (fail "no head") return dat
  if Base16.encode old == dat' then do
    -- FIXME: exceptions here should be caught
    result <- Zoo.set zk "/head" (Just (Base16.encode new)) (fromIntegral (Zoo.stat_version stat))
    return True
   else
    return False

getHead :: ZkInterface -> IO (Maybe B.ByteString)
getHead (ZkInterface zk) = do
  (dat, _) <- Zoo.get zk "/head" Zoo.NoWatch
  head <- maybe (fail "no head") return dat
  return (Just (fst (Base16.decode head)))
