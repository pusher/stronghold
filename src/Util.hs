{-# LANGUAGE OverloadedStrings #-}
module Util (
  deepMerge,
  utcFromInteger,
  integerFromUTC,
  pathToText,
  pathToList,
  listToPath,
  Path (Path)
) where

import Data.HashMap.Strict (unionWith)
import Data.Monoid (Monoid, mempty, mappend)
import Data.Text (Text)
import Data.Time.Clock (UTCTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime, utcTimeToPOSIXSeconds)

import qualified Data.Aeson as Aeson
import qualified Data.Text as Text

newtype Path = Path [Text]

deepMerge :: Aeson.Value -> Aeson.Value -> Aeson.Value
deepMerge (Aeson.Object a) (Aeson.Object b) =
  Aeson.Object $ unionWith deepMerge a b
deepMerge _ x = x

utcFromInteger :: Integer -> UTCTime
utcFromInteger = posixSecondsToUTCTime . fromIntegral

integerFromUTC :: UTCTime -> Integer
integerFromUTC = round . utcTimeToPOSIXSeconds

pathToText :: Path -> Text
pathToText (Path p) = Text.concat (concatMap (\x -> ["/", x]) p)

pathToList :: Path -> [Text]
pathToList (Path p) = p

listToPath :: [Text] -> Path
listToPath = Path

instance Monoid Path where
  mempty = Path []
  mappend (Path x) (Path y) = Path (x ++ y)
