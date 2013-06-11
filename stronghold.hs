{-# LANGUAGE DataKinds, OverloadedStrings #-}
module Main where

{-
  This file should only define Stronghold's API.
-}

import Data.Monoid (mempty)
import Data.Maybe (fromJust, listToMaybe)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Base16 as Base16
import Data.Text (Text, unpack)
import qualified Data.Text as Text
import Data.Text.Encoding (decodeUtf8)
import qualified Data.Aeson as Aeson
import qualified Data.HashMap.Strict as HashMap
import Data.Time.Clock (getCurrentTime, UTCTime)

import Control.Applicative ((<$>), (<|>), empty)
import Control.Monad (foldM, join)
import Control.Monad.IO.Class (liftIO)
import Control.Exception (tryJust, try, SomeException)

import Snap.Core
import Snap.Http.Server

import Crypto.Hash.SHA1 (hash)

import System.Environment (getArgs)

import qualified ZkInterface as Zk
import StoredData
import Trees
import Util (deepMerge, utcFromInteger)

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

sendError :: HTTPStatus -> Text -> Snap ()
sendError status body = do
  modifyResponse $ setResponseStatus (errorCode status) (errorMessage status)
  writeText body
  writeText "\n"

finish :: Snap a
finish = getResponse >>= finishWith

runStoreOpSnap :: Zk.ZkInterface -> StoreOp a -> Snap a
runStoreOpSnap zk op = do
  result <- liftIO $ runStoreOp' zk op
  case result of
    Nothing -> do
      sendError InternalServerError "There was some Zookeeper related error"
      finish
    Just x -> return x

site :: Zk.ZkInterface -> Snap ()
site zk =
  ifTop (writeBS "Stronghold say hi") <|>
  route [
    ("head", fetchHead),
    ("versions", versions),
    (":version/", withVersion)
   ]
 where
  withVersion :: Snap ()
  withVersion = do
    Just version <- getParam "version"
    ref <- runStoreOpSnap zk (createRef version)
    case ref of
      Just ref' ->
        let hist = makeHistoryTree ref' in
          route [
            ("tree/paths", paths hist),
            ("tree/materialized/", materialized hist),
            ("tree/peculiar/", peculiar hist),
            ("change", info ref'),
            ("next/tree/materialized", next ref'),
            ("update/", update ref')
           ]
      Nothing -> sendError UnprocessableEntity "Invalid reference"

  fetchHead :: Snap ()
  fetchHead = ifTop $ method GET $ do
    head <- runStoreOpSnap zk getHead
    writeBS (unref head)

  fetchAt :: UTCTime -> Snap ()
  fetchAt ts = do
    ref <- runStoreOpSnap zk $ findActive ts
    case ref of
      Nothing -> writeText ""
      Just ref' -> writeBS (unref ref')

  recordToJSON :: Ref HistoryTag -> MetaInfo -> JSON
  recordToJSON ref (MetaInfo ts comment author) =
    Aeson.object [
      ("revision", Aeson.toJSON (unref ref)),
      ("timestamp", Aeson.toJSON ts),
      ("comment", Aeson.toJSON comment),
      ("author", Aeson.toJSON author)
     ]

  fetchN :: Maybe Int -> Ref HistoryTag -> Snap ()
  fetchN limit ref = do
    result <- runStoreOpSnap zk $ fetchHistory limit ref
    let result' = map (uncurry recordToJSON) result
    writeLBS $ Aeson.encode result'

  maybeRead :: Read a => String -> Maybe a
  maybeRead = fmap fst . listToMaybe . reads

  -- query the set of versions
  versions :: Snap ()
  versions = ifTop $ method GET $ do
    ts <- getParam "at"
    case ts of
      Just ts' -> do
        ts'' <- (maybe empty return . fmap utcFromInteger . maybeRead . unpack . decodeUtf8) ts'
        fetchAt ts''
      Nothing -> do
        last <- getParam "last"
        last' <- maybe empty return last
        last'' <- runStoreOpSnap zk $ createRef last'
        case last'' of
          Just last''' -> do
            size <- getParam "size"
            let size' = (join  . fmap (maybeRead . unpack . decodeUtf8)) size
            fetchN size' last'''
          Nothing -> sendError UnprocessableEntity "Invalid reference"

  paths :: History -> Snap ()
  paths hist = ifTop $ method GET $ do
    paths <- runStoreOpSnap zk $ do
      hist' <- derefHistory hist
      case hist' of
        Nil -> return []
        Cons (_, hier) _ -> do
          z <- makeHierarchyZipper hier
          subPaths z
    writeLBS (Aeson.encode paths)

  next :: Ref HistoryTag -> Snap ()
  next hist = method GET $ do
    extendTimeout 300
    path <- rqPathInfo <$> getRequest
    let parts = Text.splitOn "/" (decodeUtf8 path)
    result <- runStoreOpSnap zk $ nextMaterializedView hist parts
    (json, revision) <- maybe (liftIO $ fail "") return result
    let object = [("data", json), ("revision", Aeson.String (decodeUtf8 (unref revision)))]
    writeLBS $ Aeson.encode $ Aeson.object object

  renderPath :: [Text] -> Text
  renderPath = Text.concat . concatMap (\x -> ["/", x])

  formatChanges :: [([Text], JSON, JSON)] -> JSON
  formatChanges =
    Aeson.toJSON . map (\(path, old, new) ->
      Aeson.object [
        ("path", Aeson.toJSON (renderPath path)),
        ("old", old),
        ("new", new)
      ])

  formatInfo :: (MetaInfo, Ref HistoryTag, [([Text], JSON, JSON)]) -> JSON
  formatInfo (meta, previous, changes) =
    deepMerge
      (Aeson.toJSON meta)
      (Aeson.object [
        ("previous", Aeson.toJSON (unref previous)),
        ("changes", formatChanges changes)
      ])

  info :: Ref HistoryTag -> Snap ()
  info ref = ifTop $ method GET $ do
    result <- runStoreOpSnap zk $ loadInfo ref
    writeLBS $ Aeson.encode $ fmap formatInfo result

  materialized :: History -> Snap ()
  materialized hist = method GET $ do
    path <- rqPathInfo <$> getRequest
    let parts = Text.splitOn "/" (decodeUtf8 path)
    json <- runStoreOpSnap zk $ do
      hier <- lastHierarchy hist
      snd <$> materializedView parts hier
    writeLBS $ Aeson.encode $ json

  peculiar :: History -> Snap ()
  peculiar hist = method GET $ do
    path <- rqPathInfo <$> getRequest
    let parts = Text.splitOn "/" (decodeUtf8 path)
    json <- runStoreOpSnap zk $ do
      hier <- lastHierarchy hist
      z <- makeHierarchyZipper hier
      z' <- followPath parts z
      (_, json) <- getJSON z'
      return json
    writeLBS $ Aeson.encode json

  resultToMaybe :: Aeson.Result x -> Maybe x
  resultToMaybe (Aeson.Success x) = Just x
  resultToMaybe _ = Nothing

  jsonLookupText :: Text -> Aeson.Object -> Maybe Text
  jsonLookupText key obj = do
    field <- HashMap.lookup key obj
    resultToMaybe $ Aeson.fromJSON field

  retrieveUpdateInfo :: JSON -> Maybe (Text, Text, JSON)
  retrieveUpdateInfo val = do
    obj <- resultToMaybe $ Aeson.fromJSON val
    author <- jsonLookupText "author" obj
    comment <- jsonLookupText "comment" obj
    dat <- HashMap.lookup "data" obj
    Aeson.Object _ <- resultToMaybe $ Aeson.fromJSON dat -- make sure that data is an object
    return (author, comment, dat)

  update :: Ref HistoryTag -> Snap ()
  update ref = method POST $ do
    path <- rqPathInfo <$> getRequest
    let parts = Text.splitOn "/" (decodeUtf8 path)
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
            result <- (runStoreOpSnap zk $ updateHierarchy meta parts dat ref)
            case result of
              Just head -> writeBS (unref head)
              Nothing ->
                sendError Conflict "The update was aborted because an ancestor or descendent has changed"

main :: IO ()
main = do
  [portString] <- getArgs
  zk <- Zk.newZkInterface "localhost:2181"
  simpleHttpServe (setPort (read portString) mempty :: Config Snap ()) (site zk)
