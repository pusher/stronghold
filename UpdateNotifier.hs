{-# LANGUAGE GADTs, DataKinds #-}
module UpdateNotifier where

import Data.Maybe (maybe)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.Text (Text)
import Data.Hashable (Hashable)

import Control.Applicative ((<$>))
import Control.Monad (forever, foldM, forM_, when)
import Control.Lens
import Control.Concurrent (forkIO)
import Control.Concurrent.Async (Async, async)
import Control.Concurrent.Async as Async
import Control.Concurrent.STM (STM, atomically, retry, TVar, readTVar, writeTVar, newTVar, newTVarIO, readTVarIO)

import StoredData
import Trees
import ZkInterface as Zk

{-
  The job of an UpdateNotifier is to provide a way of being notified of
  materializedView view updates.

  The way that it works is, when you want to be alerted, you add a
  `TVar (Maybe JSON)` containing `Nothing` to a HashMap in another TVar. 
  A thread, that is started when the UpdateNotifier is created, waits for 
  updates and when they occur, it computes the new materializedView for the paths 
  in the HashMap. Where these have changed, the associated `TVar (Maybe JSON)`s
  are filled, otherwise they are migrated to the new HashMap.
-}

data UpdateNotifier =
  UpdateNotifier
    !Zk.ZkInterface
    !(TVar (Maybe (Ref HistoryTag, HashMap [Text] (Maybe JSON, [TVar (Maybe JSON)]))))

newUpdateNotifier :: ZkInterface -> IO UpdateNotifier
newUpdateNotifier zk = do
  waiting <- newTVarIO Nothing
  forkIO $ forever $ do
    x <- atomically $ do
      upstreamRef <- makeRef <$> getHeadSTM zk
      waiting' <- readTVar waiting
      case waiting' of
        Nothing -> do
          writeTVar waiting (Just (upstreamRef, HashMap.empty))
          return Nothing
        Just (waitingRef, waitingMap) ->
          if waitingRef == upstreamRef then
            retry
          else do
            writeTVar waiting (Just (upstreamRef, HashMap.empty))
            return $ Just (upstreamRef, waitingRef, waitingMap)
    case x of
      Nothing -> return ()
      Just (upstreamRef, waitingRef, waitingMap) -> do
        (waitingMap'', fillList) <- runStoreOp zk $ do
          waitingMap' <- fillJSONs waitingRef waitingMap
          revisions <- revisionsBetween waitingRef upstreamRef
          case revisions of
            Nothing -> fail "a non-monotonic change has been made"
            Just revisions' -> handleRevisions revisions' waitingMap'
        atomically $
          forM_ fillList (\(json, tvars) ->
            forM_ tvars (flip writeTVar (Just json)))
        atomically $ do
          Just (waitingRef', waitingMap''') <- readTVar waiting
          when (waitingRef /= waitingRef') $ fail "This is bad"
          writeTVar waiting (Just (waitingRef, mergeWaitingMap waitingMap'' waitingMap'''))
  return $ UpdateNotifier zk waiting
 where
  fillJSONs :: Ref HistoryTag -> HashMap [Text] (Maybe JSON, x) -> StoreOp (HashMap [Text] (JSON, x))
  fillJSONs ref h = do
    hier <- hierarchyFromRevision ref
    fst <$> foldM (\(m, hier) (k, (json, x)) ->
      case json of
        Nothing -> do
          (hier', json') <- materializedView k hier
          return (HashMap.insert k (json', x) m, hier')
        Just json' ->
          return (HashMap.insert k (json', x) m, hier)) (HashMap.empty, hier) (HashMap.toList h)

  mergeWaitingMap :: (Hashable k, Eq k) =>
    HashMap k (x, [y]) -> HashMap k (x, [y]) -> HashMap k (x, [y])
  mergeWaitingMap = HashMap.unionWith (\(a, b) (_, c) -> (a, b ++ c))

  handleRevisions ::
    [Ref HistoryTag] -> HashMap [Text] (JSON, [x]) ->
    StoreOp (HashMap [Text] (Maybe JSON, [x]), [(JSON, [x])])
  handleRevisions revisions m = do
    (m', fillList) <- foldM (\(newMap, fillList) revision -> do
      (newMap', fillList') <- handleRevision revision newMap
      return (newMap', fillList' ++ fillList)) (m, []) revisions
    return (HashMap.map (\(x, y) -> (Just x, y)) m, fillList)

  handleRevision ::
    Ref HistoryTag -> HashMap [Text] (JSON, [x]) ->
    StoreOp (HashMap [Text] (JSON, [x]), [(JSON, [x])])
  handleRevision revision m = do
    hier <- hierarchyFromRevision revision
    (_, newMap, fillList) <- foldM (\(hier, newMap, fillList) (path, (json, tvars)) -> do
      (hier', json') <- materializedView path hier
      if json == json' then
        return (hier', HashMap.insert path (json, tvars) newMap, fillList)
       else
        return (hier', newMap, (json', tvars):fillList))
          (hier, HashMap.empty, []) (HashMap.toList m)
    return (newMap, fillList)

nextMaterializedView :: UpdateNotifier -> Ref HistoryTag -> [Text] -> IO (Async JSON)
nextMaterializedView notifier@(UpdateNotifier zk waiting) ref path = do
  x <- atomically $ do
    upstreamRef <- makeRef <$> getHeadSTM zk
    if upstreamRef == ref then do
      waiting' <- readTVar waiting
      case waiting' of
        Nothing -> retry
        Just x@(waitingRef, waitingMap) ->
          if waitingRef == upstreamRef then do
            t <- newTVar Nothing
            let waiting'' = Just $ over (_2 . at path) (Just . maybe (Nothing, [t]) (over _2 (t :))) x
            writeTVar waiting waiting''
            return (Right t)
          else
            retry
     else
      return (Left upstreamRef)
  case x of
    Left head -> do
      result <- runStoreOp zk $ do
        revisions <- revisionsBetween ref head
        case revisions of
          Nothing -> fail "ref not in head"
          Just revisions' -> do
            h <- hierarchyFromRevision ref
            (_, json) <- materializedView path h
            foldM (\answer revision ->
              case answer of
                Just _ -> return answer
                Nothing -> do
                  hier <- hierarchyFromRevision revision
                  (_, json') <- materializedView path hier
                  if json == json' then
                    return Nothing
                   else
                    return (Just json')) Nothing revisions'
      case result of
        Just materialized -> async $ return materialized
        Nothing -> nextMaterializedView notifier head path
    Right tvar ->
      async $ atomically $ readTVar tvar >>= maybe retry return
