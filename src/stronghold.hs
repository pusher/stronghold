{-# LANGUAGE DataKinds, OverloadedStrings, Rank2Types #-}
module Main where

{-
  This file should only define Stronghold's API.
-}

import Control.Applicative ((<$>), (<*>), (<|>), empty)
import Control.Concurrent (myThreadId, throwTo)
import Control.Exception (SomeException, try, tryJust)
import Control.Monad (foldM, join)
import Control.Monad.IO.Class (liftIO)
import Crypto.Hash.SHA1 (hash)
import Data.ByteString (ByteString)
import Data.CaseInsensitive (CI)
import Data.Maybe (fromMaybe, fromJust, listToMaybe)
import Data.Monoid (Endo(Endo), appEndo, mempty, mconcat)
import Data.Text (Text, unpack)
import Data.Text.Encoding (decodeUtf8)
import Data.Time.Clock (UTCTime, getCurrentTime)
import Data.Traversable (mapM)
import Prelude hiding (mapM)
import Snap.Core (Method(GET, POST), Snap)
import StoredData (JSON)
import System.Environment (getArgs)
import System.IO (Handle, stderr, stdout)
import Util (Path)

import qualified Data.Aeson as Aeson
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Base16 as Base16
import qualified Data.ByteString.Char8 as BC
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as Text
import qualified Snap.Core as Snap
import qualified Snap.Http.Server as Server
import qualified StoredData as SD
import qualified SQLiteInterface as SQL
import qualified Trees
import qualified Util
import qualified ZkInterface as Zk

data HTTPStatus =
  BadRequest |
  Conflict |
  UnprocessableEntity |
  InternalServerError

-- HTTP Header to return the revision of the changeset
etagHeader :: CI ByteString
etagHeader = "ETag"

errorCode :: HTTPStatus -> Int
errorCode BadRequest = 400
errorCode Conflict = 409
errorCode UnprocessableEntity = 422
errorCode InternalServerError = 500

errorMessage :: HTTPStatus -> ByteString
errorMessage InternalServerError = "Internal Server Error"
errorMessage BadRequest = "Bad Request"
errorMessage Conflict = "Conflict"
errorMessage UnprocessableEntity = "Unprocessable Entity"

sendError :: HTTPStatus -> Text -> Snap a
sendError status body = do
  Snap.modifyResponse $
    Snap.setResponseStatus (errorCode status) (errorMessage status)
  Snap.writeText body
  Snap.writeText "\n"
  Snap.getResponse >>= Snap.finishWith

site :: (forall a. SD.StoreOp a -> Snap a) -> Snap ()
site runStoreOp =
  Snap.ifTop (Snap.writeBS "Stronghold says hi") <|>
  Snap.route [
    ("head", fetchHead),
    ("versions", versions),
    (":version/", withVersion)
   ]
 where
  createRef' :: ByteString -> Snap (SD.Ref SD.HistoryTag)
  createRef' b = do
    b' <- runStoreOp $ SD.createRef b
    case b' of
      Nothing -> sendError UnprocessableEntity "Invalid reference"
      Just b'' -> return b''

  withVersion :: Snap ()
  withVersion = do
    Just version <- Snap.getParam "version"
    ref <- createRef' version
    let hist = Trees.makeHistoryTree ref
    Snap.route [
      ("tree/paths", paths hist),
      ("tree/materialized", materialized hist),
      ("tree/peculiar", peculiar hist),
      ("change", info ref),
      ("next/tree/materialized", next ref),
      ("update", update ref)
     ]

  fetchHead :: Snap ()
  fetchHead = Snap.ifTop $ Snap.method GET $ do
    head <- runStoreOp SD.getHead
    Snap.writeBS (SD.unref head)

  fetchAt :: UTCTime -> Snap ()
  fetchAt ts = do
    ref <- runStoreOp $ Trees.findActive ts
    Snap.writeBS (SD.unref ref)

  recordToJSON :: SD.Ref SD.HistoryTag -> SD.MetaInfo -> [Path] -> SD.JSON
  recordToJSON ref (SD.MetaInfo ts comment author) paths =
    Aeson.object [
      ("revision", Aeson.toJSON $ decodeUtf8 (SD.unref ref)),
      ("timestamp", Aeson.toJSON $ Util.integerFromUTC ts),
      ("comment", Aeson.toJSON comment),
      ("author", Aeson.toJSON author),
      ("paths", Aeson.toJSON (map Util.pathToText paths))
     ]

  maybeReadBS :: Read a => ByteString -> Maybe a
  maybeReadBS = fmap fst . listToMaybe . reads . unpack . decodeUtf8

  summarizeRevisions :: [SD.Ref SD.HistoryTag] -> Snap JSON
  summarizeRevisions revisions = do
    infos <- runStoreOp $ mapM Trees.loadInfo revisions
    let err = sendError UnprocessableEntity
                "can't process the sentinel history node"
    infos' <- maybe err return (sequence infos)
    let result = zip revisions infos'
    let paths = map (\(path, _, _) -> path)
    let result' = map (\(rev, (meta, _, changes)) ->
          recordToJSON rev meta (paths changes)) result
    return $ Aeson.toJSON result'

  -- query the set of versions
  versions :: Snap ()
  versions = Snap.ifTop $ Snap.method GET $ do
    at <- Snap.getParam "at"

    last <- Snap.getParam "last"
    size <- Snap.getParam "size"

    first <- Snap.getParam "first"
    limit <- Snap.getParam "limit"

    first' <- mapM createRef' first
    last' <- mapM createRef' last
    limit' <- mapM createRef' limit
    let at' = (join . fmap (fmap Util.utcFromInteger . maybeReadBS)) at
    let size' = (join . fmap maybeReadBS) size

    case (at', last', size', first', limit') of
      (Just ts, Nothing, Nothing, Nothing, Nothing) ->
        fetchAt ts
      (Nothing, Just last'', _, Nothing, Nothing) -> do
        revisions <- runStoreOp $ Trees.revisionsBefore size' last''
        result <- summarizeRevisions revisions
        Snap.writeLBS $ Aeson.encode result
      (Nothing, Nothing, Just size'', Just first'', Just limit'') ->
        if first'' == limit'' then
          Snap.writeLBS $ Aeson.encode ([] :: [JSON])
         else do
          revisions <- runStoreOp $ Trees.revisionsBetween first'' limit''
          let err = sendError UnprocessableEntity
                "first is not in limit's history"
          revisions' <- maybe err return revisions
          let revisions'' = take size'' revisions'
          result <- summarizeRevisions revisions''
          Snap.writeLBS $ Aeson.encode result
      (_, _, _, _, _) -> empty

  paths :: Trees.History -> Snap ()
  paths hist = Snap.ifTop $ Snap.method GET $ do
    paths <- runStoreOp $ do
      hist' <- Trees.derefHistory hist
      case hist' of
        SD.Nil -> return []
        SD.Cons (_, hier) _ -> do
          z <- Trees.makeHierarchyZipper hier
          Trees.subPaths z
    Snap.writeLBS (Aeson.encode (map Util.pathToText paths))

  getPath :: Snap Path
  getPath = do
    path <- (decodeUtf8 . Snap.rqPathInfo) <$> Snap.getRequest
    if Text.null path then
      return mempty
     else if Text.last path == '/' then
      fail "couldn't construct path"
     else
      (return . Util.listToPath . Text.splitOn "/") path

  next :: SD.Ref SD.HistoryTag -> Snap ()
  next hist = Snap.method GET $ do
    Snap.extendTimeout 300
    path <- getPath
    result <- runStoreOp $ Trees.nextMaterializedView hist path
    (json, revision) <- maybe (liftIO $ fail "") return result
    let object = [("data", json),
                  ("revision", Aeson.String (decodeUtf8 (SD.unref revision)))]
    Snap.writeLBS $ Aeson.encode $ Aeson.object object

  formatChanges :: [(Path, JSON, JSON)] -> JSON
  formatChanges =
    Aeson.toJSON . map (\(path, old, new) ->
      Aeson.object [
        ("path", Aeson.toJSON (Util.pathToText path)),
        ("old", old),
        ("new", new)
      ])

  formatInfo
    :: Maybe (SD.MetaInfo, SD.Ref SD.HistoryTag, [(Path, JSON, JSON)])
    -> JSON
  formatInfo (Just (meta, previous, changes)) =
    Util.deepMerge
      (Aeson.toJSON meta)
      (Aeson.object [
        ("previous", Aeson.toJSON $ decodeUtf8 (SD.unref previous) ),
        ("changes", formatChanges changes)
      ])
  formatInfo Nothing = Aeson.object [("previous", Aeson.Null)]

  info :: SD.Ref SD.HistoryTag -> Snap ()
  info ref = Snap.ifTop $ Snap.method GET $ do
    result <- runStoreOp $ Trees.loadInfo ref
    Snap.writeLBS $ Aeson.encode $ formatInfo result

  materialized :: Trees.History -> Snap ()
  materialized hist = Snap.method GET $ do
    path <- getPath
    json <- runStoreOp $ do
      hier <- Trees.lastHierarchy hist
      snd <$> Trees.materializedView path hier

    let jsonBS = toStrict $ Aeson.encode json
    let etag = Base16.encode $ hash jsonBS

    Snap.modifyResponse $ Snap.setHeader etagHeader etag
    Snap.writeBS jsonBS
   where
     -- BS.toStrict is available in more moderns versions of GHC
     toStrict = BS.concat . LBS.toChunks

  peculiar :: Trees.History -> Snap ()
  peculiar hist = Snap.method GET $ do
    path <- getPath
    json <- runStoreOp $ do
      hier <- Trees.lastHierarchy hist
      z <- Trees.makeHierarchyZipper hier
      z' <- Trees.followPath path z
      (_, json) <- Trees.getJSON z'
      return json
    Snap.writeLBS $ Aeson.encode json

  resultToMaybe :: Aeson.Result x -> Maybe x
  resultToMaybe (Aeson.Success x) = Just x
  resultToMaybe _ = Nothing

  jsonLookup :: Aeson.FromJSON a => Text -> Aeson.Object -> Maybe a
  jsonLookup key obj = do
    field <- HM.lookup key obj
    resultToMaybe $ Aeson.fromJSON field

  retrieveUpdateInfo :: JSON -> Maybe (Text, Text, JSON)
  retrieveUpdateInfo val = do
    obj <- resultToMaybe $ Aeson.fromJSON val
    dat <- jsonLookup "data" obj
    -- ensure that data is an object
    resultToMaybe $ Aeson.fromJSON dat :: Maybe Aeson.Object
    (,,) <$>
      jsonLookup "author" obj <*>
      jsonLookup "comment" obj <*> return dat

  update :: SD.Ref SD.HistoryTag -> Snap ()
  update ref = Snap.method POST $ do
    path <- getPath
    body <- Snap.readRequestBody 102400
    case Aeson.decode body of
      Nothing ->
        sendError BadRequest "Invalid JSON"
      Just body' ->
        case retrieveUpdateInfo body' of
          Nothing ->
            sendError UnprocessableEntity
              "The given JSON should contain: author, comment, data"
          Just (author, comment, dat) -> do
            ts <- liftIO getCurrentTime
            let meta = SD.MetaInfo ts comment author
            result <- runStoreOp $ Trees.updateHierarchy meta path dat ref
            case result of
              Just head -> Snap.writeBS (SD.unref head)
              Nothing ->
                sendError Conflict
                  "The update was aborted because an ancestor or descendent has changed"

writeTo :: Handle -> Server.ConfigLog
writeTo handle = Server.ConfigIoLog (BC.hPutStrLn handle)

applyAll :: [a -> a] -> a -> a
applyAll = appEndo . mconcat . map Endo

runStoreOpSnapZk :: Zk.ZkInterface -> SD.StoreOp a -> Snap a
runStoreOpSnapZk zk op = do
  result <- liftIO $ Zk.runStoreOp zk op
  case result of
    Nothing ->
      sendError InternalServerError "There was some Zookeeper related error"
    Just x -> return x

runStoreOpSnapSQL :: SQL.SQLiteInterface -> SD.StoreOp a -> Snap a
runStoreOpSnapSQL sql op = do
  result <- liftIO $ SQL.runStoreOp sql op
  case result of
    Nothing ->
      sendError InternalServerError "There was some SQLite related problem"
    Just x -> return x

main :: IO ()
main = do
  args <- getArgs
  case args of
    [portString, zkHostPort] -> do
      thread <- myThreadId
      zk <- Zk.newZkInterface zkHostPort (throwTo thread)
      start (runStoreOpSnapZk zk) (read portString)
    ["development", portString, path] -> do
      sql <- SQL.newSQLiteInterface path
      start (runStoreOpSnapSQL sql) (read portString)
    _ -> putStrLn "Expected stronghold [port] [zookeeper host string]"

start :: (forall a. SD.StoreOp a -> Snap a) -> Int -> IO ()
start runStoreOp port = do
  let configOpts = [
          Server.setPort port,
          Server.setAccessLog (writeTo stdout),
          Server.setErrorLog (writeTo stderr)
        ]
      config = applyAll configOpts Server.defaultConfig
  Server.simpleHttpServe (config :: Server.Config Snap ()) (site runStoreOp)
