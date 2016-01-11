{-# LANGUAGE OverloadedStrings, GADTs #-}
module SQLiteInterface where

import Control.Concurrent.STM (TVar, STM, atomically, retry)
import Control.Monad (when)
import Control.Monad.Operational (ProgramViewT (..), view)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Maybe (MaybeT (..))
import Crypto.Hash.SHA1 (hash)
import Data.ByteString (ByteString)
import Data.Maybe (isJust, listToMaybe )
import Data.Serialize (decode, encode)
import StoredData (Data (..), ListNode (..), StoreInstr (..), StoreOp)

import qualified Control.Concurrent.STM as STM
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as Base16
import qualified Database.SQLite.Simple as SQL
import qualified StoredData as SD

data SQLiteInterface = SQLiteInterface (TVar ByteString) SQL.Connection

nilNode = encode (HistoryNode Nil)
nilHash = (Base16.encode . hash) nilNode

populateEmpty :: SQL.Connection -> IO ByteString
populateEmpty conn = do
  mapM_
    (SQL.execute conn "INSERT INTO refs values (?, ?)")
    [("head", nilHash), (nilHash, nilNode)]
  return nilHash

newSQLiteInterface :: FilePath -> IO SQLiteInterface
newSQLiteInterface filename = do
  conn <- SQL.open filename
  SQL.execute_
    conn
    "CREATE TABLE IF NOT EXISTS refs (key string primary key, value string)"
  head <-
    SQL.query
      conn
      "SELECT value from refs where key=?"
      (SQL.Only ("head" :: ByteString))
  head' <- maybe (populateEmpty conn) (return . SQL.fromOnly) (listToMaybe head)
  var <- STM.newTVarIO head'
  return (SQLiteInterface var conn)

getHeadSTM :: SQLiteInterface -> STM ByteString
getHeadSTM (SQLiteInterface var _) = STM.readTVar var

getHead :: SQLiteInterface -> MaybeT IO B.ByteString
getHead sql = lift $ atomically $ getHeadSTM sql

getHeadBlockIfEq :: SQLiteInterface -> B.ByteString -> MaybeT IO B.ByteString
getHeadBlockIfEq sql ref = lift $ atomically $ do
  head <- getHeadSTM sql
  if head == ref then
    retry
   else
    return head

updateHead :: SQLiteInterface -> ByteString -> ByteString -> MaybeT IO Bool
updateHead (SQLiteInterface var conn) old new = lift $ do
  b <- atomically $ do
    head <- STM.readTVar var
    if head == old then do
      STM.writeTVar var new
      return True
     else
      return False
  when b $
    SQL.execute
      conn
      "UPDATE refs set value=? where key=?"
      (new, "head" :: ByteString)
  return b

loadData :: SQLiteInterface -> ByteString -> MaybeT IO ByteString
loadData (SQLiteInterface var conn) ref = MaybeT $ do
  value <- SQL.query conn "SELECT value from refs where key=?" (SQL.Only ref)
  (return . fmap SQL.fromOnly . listToMaybe) value

storeData :: SQLiteInterface -> ByteString -> MaybeT IO ByteString
storeData (SQLiteInterface var conn) value = lift $ do
  let ref = (Base16.encode . hash) value
  SQL.execute conn "INSERT OR IGNORE INTO refs values (?, ?)" (ref, value)
  return ref

hasReference :: SQLiteInterface -> ByteString -> MaybeT IO Bool
hasReference sql ref =
  (fmap isJust . lift . runMaybeT) (loadData sql ref)

runStoreOp' :: SQLiteInterface -> StoreOp a -> MaybeT IO a
runStoreOp' sql op =
  case view op of
    Return x -> return x
    Store d :>>= rest -> do
      let d' = encode d
      ref <- storeData sql d'
      runStoreOp' sql (rest (SD.makeRef ref))
    Load r :>>= rest -> do
      dat <- loadData sql (SD.unref r)
      either fail (runStoreOp' sql . rest) (decode dat)
    GetHead :>>= rest -> do
      head <- getHead sql
      runStoreOp' sql (rest (SD.makeRef head))
    GetHeadBlockIfEq ref :>>= rest -> do
      head <- getHeadBlockIfEq sql (SD.unref ref)
      runStoreOp' sql (rest (SD.makeRef head))
    UpdateHead old new :>>= rest -> do
      b <- updateHead sql (SD.unref old) (SD.unref new)
      runStoreOp' sql (rest b)
    CreateRef r :>>= rest -> do
      b <- hasReference sql r
      if b then
        runStoreOp' sql (rest (Just (SD.makeRef r)))
       else
        runStoreOp' sql (rest Nothing)

runStoreOp :: SQLiteInterface -> StoreOp a -> IO (Maybe a)
runStoreOp sql op = runMaybeT (runStoreOp' sql op)
