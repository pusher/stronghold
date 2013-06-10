module Util (
  deepMerge
) where

import qualified Data.Aeson as Aeson
import Data.HashMap.Strict (unionWith)

deepMerge :: Aeson.Value -> Aeson.Value -> Aeson.Value
deepMerge (Aeson.Object a) (Aeson.Object b) = Aeson.Object $ unionWith deepMerge a b
deepMerge _ x = x
