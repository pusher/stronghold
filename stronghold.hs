{-# LANGUAGE DataKinds, OverloadedStrings #-}
module Main where

{-
  This file should only define Stronghold's API.
-}

import Data.Monoid (mempty)
import Data.Maybe (fromJust)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Base16 as Base16
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Text.Encoding (decodeUtf8)
import qualified Data.Aeson as Aeson
import qualified Data.HashMap.Strict as HashMap
import Data.Time.Clock (getCurrentTime)

import Control.Applicative ((<$>), (<|>))
import Control.Monad (foldM)
import Control.Monad.IO.Class (liftIO)
import Control.Exception (tryJust, try, SomeException)
import Control.Concurrent.Async (wait)

import Snap.Core
import Snap.Http.Server

import Crypto.Hash.SHA1 (hash)

import System.Environment (getArgs)

import qualified ZkInterface as Zk
import StoredData
import Trees
import UpdateNotifier

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
errorMessage BadRequest = "Bad Request"
errorMessage Conflict = "Conflict"
errorMessage UnprocessableEntity = "Unprocessable Entity"

sendError :: HTTPStatus -> Text -> Snap ()
sendError status body = do
  modifyResponse $ setResponseStatus (errorCode status) (errorMessage status)
  writeText body
  writeText "\n"

site :: Zk.ZkInterface -> UpdateNotifier -> Snap ()
site zk notifier =
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
    ref <- liftIO $ runStoreOp zk (createRef version)
    case ref of
      Just ref' ->
        let hist = makeHistoryTree ref' in
          route [
            ("tree/paths", paths hist),
            ("tree/materialized/", materialized hist),
            ("tree/peculiar/", peculiar hist),
            ("change", info hist),
            ("next/tree/materialized", next ref'),
            ("update/", update ref')
           ]
      Nothing -> sendError UnprocessableEntity "Invalid reference"

  fetchHead :: Snap ()
  fetchHead = ifTop $ method GET $ do
    head <- liftIO $ runStoreOp zk getHead
    writeBS (unref head)

  -- query the set of versions
  versions :: Snap ()
  versions = undefined

  paths :: History -> Snap ()
  paths hist = ifTop $ method GET $ do
    paths <- liftIO $ runStoreOp zk $ do
      hist' <- derefHistory hist
      case hist' of
        Nil -> return []
        Cons (_, hier) _ -> do
          z <- makeHierarchyZipper hier
          subPaths z
    writeLBS (Aeson.encode paths)

  next :: Ref HistoryTag -> Snap ()
  next hist = method GET $ do
    path <- rqPathInfo <$> getRequest
    let parts = Text.splitOn "/" (decodeUtf8 path)
    future <- liftIO $ nextMaterializedView notifier hist parts
    json <- liftIO $ wait $ future
    writeLBS $ Aeson.encode json

  info :: History -> Snap ()
  info hist = ifTop $ method GET $ do
    meta <- liftIO $ runStoreOp zk $ lastMetaInfo hist
    -- TODO: include the previous reference
    writeLBS $ Aeson.encode meta

  materialized :: History -> Snap ()
  materialized hist = method GET $ do
    path <- rqPathInfo <$> getRequest
    let parts = Text.splitOn "/" (decodeUtf8 path)
    json <- liftIO $ runStoreOp zk $ do
      hier <- lastHierarchy hist
      snd <$> materializedView parts hier
    writeLBS $ Aeson.encode $ json

  peculiar :: History -> Snap ()
  peculiar hist = method GET $ do
    path <- rqPathInfo <$> getRequest
    let parts = Text.splitOn "/" (decodeUtf8 path)
    json <- liftIO $ runStoreOp zk $ do
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
            result <- (liftIO $ runStoreOp zk $ updateHierarchy meta parts dat ref)
            case result of
              Just head -> writeBS (unref head)
              Nothing ->
                sendError Conflict "The update was aborted because an ancestor or descendent has changed"

main :: IO ()
main = do
  [portString] <- getArgs
  zk <- Zk.newZkInterface "localhost:2181"
  notifier <- newUpdateNotifier zk
  simpleHttpServe (setPort (read portString) mempty :: Config Snap ()) (site zk notifier)
