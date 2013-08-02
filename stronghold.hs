{-# LANGUAGE DataKinds, OverloadedStrings #-}
module Main where

{-
  This file should only define Stronghold's API.
-}

import Prelude hiding (mapM)

import Data.Monoid (mempty, mconcat, Endo(Endo), appEndo)
import Data.Maybe (fromJust, listToMaybe)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Base16 as Base16
import Data.Text (Text, unpack)
import qualified Data.Text as Text
import Data.Text.Encoding (decodeUtf8)
import qualified Data.Aeson as Aeson
import qualified Data.HashMap.Strict as HashMap
import Data.Time.Clock (getCurrentTime, UTCTime)
import Data.Traversable (mapM)

import Control.Applicative ((<$>), (<*>), (<|>), empty)
import Control.Monad (foldM, join)
import Control.Monad.IO.Class (liftIO)
import Control.Exception (tryJust, try, SomeException)

import Snap.Core
import Snap.Http.Server

import Crypto.Hash.SHA1 (hash)

import System.Environment (getArgs)
import System.IO (Handle, stdout, stderr)

import qualified ZkInterface as Zk
import StoredData
import Trees
import Util (deepMerge, integerFromUTC, utcFromInteger, Path, pathToText, listToPath)

data HTTPStatus =
  BadRequest |
  Conflict |
  UnprocessableEntity |
  InternalServerError

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
  modifyResponse $ setResponseStatus (errorCode status) (errorMessage status)
  writeText body
  writeText "\n"
  getResponse >>= finishWith

runStoreOpSnap :: Zk.ZkInterface -> StoreOp a -> Snap a
runStoreOpSnap zk op = do
  result <- liftIO $ runStoreOp' zk op
  case result of
    Nothing ->
      sendError InternalServerError "There was some Zookeeper related error"
    Just x -> return x

site :: Zk.ZkInterface -> Snap ()
site zk =
  ifTop (writeBS "Stronghold says hi") <|>
  route [
    ("head", fetchHead),
    ("versions", versions),
    (":version/", withVersion)
   ]
 where
  createRef' :: ByteString -> Snap (Ref HistoryTag)
  createRef' b = do
    b' <- runStoreOpSnap zk $ createRef b
    case b' of
      Nothing -> sendError UnprocessableEntity "Invalid reference"
      Just b'' -> return b''

  withVersion :: Snap ()
  withVersion = do
    Just version <- getParam "version"
    ref <- createRef' version
    let hist = makeHistoryTree ref
    route [
      ("tree/paths", paths hist),
      ("tree/materialized", materialized hist),
      ("tree/peculiar", peculiar hist),
      ("change", info ref),
      ("next/tree/materialized", next ref),
      ("update", update ref)
     ]

  fetchHead :: Snap ()
  fetchHead = ifTop $ method GET $ do
    head <- runStoreOpSnap zk getHead
    writeBS (unref head)

  fetchAt :: UTCTime -> Snap ()
  fetchAt ts = do
    ref <- runStoreOpSnap zk $ findActive ts
    writeBS (unref ref)

  recordToJSON :: Ref HistoryTag -> MetaInfo -> [Path] -> JSON
  recordToJSON ref (MetaInfo ts comment author) paths =
    Aeson.object [
      ("revision", Aeson.toJSON (unref ref)),
      ("timestamp", Aeson.toJSON $ integerFromUTC ts),
      ("comment", Aeson.toJSON comment),
      ("author", Aeson.toJSON author),
      ("paths", Aeson.toJSON (map pathToText paths))
     ]

  maybeReadBS :: Read a => ByteString -> Maybe a
  maybeReadBS = fmap fst . listToMaybe . reads . unpack . decodeUtf8

  summarizeRevisions :: [Ref HistoryTag] -> Snap JSON
  summarizeRevisions revisions = do
    infos <- runStoreOpSnap zk $ mapM loadInfo revisions
    let err = sendError UnprocessableEntity "can't process the sentinel history node"
    infos' <- maybe err return (sequence infos)
    let result = zip revisions infos'
    let paths = map (\(path, _, _) -> path)
    let result' = map (\(rev, (meta, _, changes)) -> recordToJSON rev meta (paths changes)) result
    return $ Aeson.toJSON result'

  -- query the set of versions
  versions :: Snap ()
  versions = ifTop $ method GET $ do
    at <- getParam "at"

    last <- getParam "last"
    size <- getParam "size"

    first <- getParam "first"
    limit <- getParam "limit"

    first' <- mapM createRef' first
    last' <- mapM createRef' last
    limit' <- mapM createRef' limit
    let at' = (join . fmap (fmap utcFromInteger . maybeReadBS)) at
    let size' = (join . fmap maybeReadBS) size

    case (at', last', size', first', limit') of
      (Just ts, Nothing, Nothing, Nothing, Nothing) -> do
        fetchAt ts
      (Nothing, Just last'', _, Nothing, Nothing) -> do
        revisions <- runStoreOpSnap zk $ revisionsBefore size' last''
        result <- summarizeRevisions revisions
        writeLBS $ Aeson.encode result
      (Nothing, Nothing, Just size'', Just first'', Just limit'') ->
        if first'' == limit'' then
          writeLBS $ Aeson.encode ([] :: [JSON])
         else do
          revisions <- runStoreOpSnap zk $ revisionsBetween first'' limit''
          let err = sendError UnprocessableEntity "first is not in limit's history"
          revisions' <- maybe err return revisions
          let revisions'' = take size'' revisions'
          result <- summarizeRevisions revisions''
          writeLBS $ Aeson.encode result
      (_, _, _, _, _) -> empty

  paths :: History -> Snap ()
  paths hist = ifTop $ method GET $ do
    paths <- runStoreOpSnap zk $ do
      hist' <- derefHistory hist
      case hist' of
        Nil -> return []
        Cons (_, hier) _ -> do
          z <- makeHierarchyZipper hier
          subPaths z
    writeLBS (Aeson.encode (map pathToText paths))

  getPath :: Snap Path
  getPath = do
    path <- (decodeUtf8 . rqPathInfo) <$> getRequest
    if Text.null path then
      return mempty
     else if Text.last path == '/' then
      fail "couldn't construct path"
     else
      (return . listToPath . Text.splitOn "/") path

  next :: Ref HistoryTag -> Snap ()
  next hist = method GET $ do
    extendTimeout 300
    path <- getPath
    result <- runStoreOpSnap zk $ nextMaterializedView hist path
    (json, revision) <- maybe (liftIO $ fail "") return result
    let object = [("data", json), ("revision", Aeson.String (decodeUtf8 (unref revision)))]
    writeLBS $ Aeson.encode $ Aeson.object object

  formatChanges :: [(Path, JSON, JSON)] -> JSON
  formatChanges =
    Aeson.toJSON . map (\(path, old, new) ->
      Aeson.object [
        ("path", Aeson.toJSON (pathToText path)),
        ("old", old),
        ("new", new)
      ])

  formatInfo :: Maybe (MetaInfo, Ref HistoryTag, [(Path, JSON, JSON)]) -> JSON
  formatInfo (Just (meta, previous, changes)) =
    deepMerge
      (Aeson.toJSON meta)
      (Aeson.object [
        ("previous", Aeson.toJSON (unref previous)),
        ("changes", formatChanges changes)
      ])
  formatInfo Nothing = Aeson.object [("previous", Aeson.Null)]

  info :: Ref HistoryTag -> Snap ()
  info ref = ifTop $ method GET $ do
    result <- runStoreOpSnap zk $ loadInfo ref
    writeLBS $ Aeson.encode $ formatInfo result

  materialized :: History -> Snap ()
  materialized hist = method GET $ do
    path <- getPath
    json <- runStoreOpSnap zk $ do
      hier <- lastHierarchy hist
      snd <$> materializedView path hier
    writeLBS $ Aeson.encode $ json

  peculiar :: History -> Snap ()
  peculiar hist = method GET $ do
    path <- getPath
    json <- runStoreOpSnap zk $ do
      hier <- lastHierarchy hist
      z <- makeHierarchyZipper hier
      z' <- followPath path z
      (_, json) <- getJSON z'
      return json
    writeLBS $ Aeson.encode json

  resultToMaybe :: Aeson.Result x -> Maybe x
  resultToMaybe (Aeson.Success x) = Just x
  resultToMaybe _ = Nothing

  jsonLookup :: Aeson.FromJSON a => Text -> Aeson.Object -> Maybe a
  jsonLookup key obj = do
    field <- HashMap.lookup key obj
    resultToMaybe $ Aeson.fromJSON field

  retrieveUpdateInfo :: JSON -> Maybe (Text, Text, JSON)
  retrieveUpdateInfo val = do
    obj <- resultToMaybe $ Aeson.fromJSON val
    dat <- jsonLookup "data" obj
    -- ensure that data is an object
    resultToMaybe $ Aeson.fromJSON dat :: Maybe Aeson.Object
    (,,) <$> jsonLookup "author" obj <*> jsonLookup "comment" obj <*> return dat

  update :: Ref HistoryTag -> Snap ()
  update ref = method POST $ do
    path <- getPath
    body <- readRequestBody 102400
    case Aeson.decode body of
      Nothing ->
        sendError BadRequest "Invalid JSON"
      Just body' ->
        case retrieveUpdateInfo body' of
          Nothing ->
            sendError UnprocessableEntity "The given JSON should contain: author, comment, data"
          Just (author, comment, dat) -> do
            ts <- liftIO $ getCurrentTime
            let meta = MetaInfo ts comment author
            result <- (runStoreOpSnap zk $ updateHierarchy meta path dat ref)
            case result of
              Just head -> writeBS (unref head)
              Nothing ->
                sendError Conflict "The update was aborted because an ancestor or descendent has changed"

writeTo :: Handle -> ConfigLog
writeTo handle = ConfigIoLog (BC.hPutStrLn handle)

applyAll :: [a -> a] -> a -> a
applyAll = appEndo . mconcat . map Endo

main :: IO ()
main = do
  [portString, zkHostPort] <- getArgs
  start (read portString) zkHostPort

start :: Int -> String -> IO ()
start port zkHostPort = do
  zk <- Zk.newZkInterface zkHostPort
  let config =
        applyAll [
          setPort port,
          setAccessLog (writeTo stdout),
          setErrorLog (writeTo stderr)
        ] defaultConfig
  simpleHttpServe (config :: Config Snap ()) (site zk)
