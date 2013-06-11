module Util (
  deepMerge,
  utcFromInteger,
  integerFromUTC
) where

import qualified Data.Aeson as Aeson
import Data.HashMap.Strict (unionWith)
import Data.Time.Clock (UTCTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime, utcTimeToPOSIXSeconds)

deepMerge :: Aeson.Value -> Aeson.Value -> Aeson.Value
deepMerge (Aeson.Object a) (Aeson.Object b) = Aeson.Object $ unionWith deepMerge a b
deepMerge _ x = x

utcFromInteger :: Integer -> UTCTime
utcFromInteger = posixSecondsToUTCTime . fromIntegral

integerFromUTC :: UTCTime -> Integer
integerFromUTC = round . utcTimeToPOSIXSeconds
