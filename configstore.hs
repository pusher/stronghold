{-# LANGUAGE GADTs, EmptyDataDecls, DataKinds, KindSignatures #-}
module Main where

import Data.ByteString (ByteString)
import Data.Text (Text)
import qualified Data.Aeson as Aeson
import Control.Monad.Operational

type JSON = Aeson.Value

type Comment = Text
type Author = Text
type ChangeSet = ()

data MetaInfo = MetaInfo Comment Author ChangeSet

data Tag =
  JSONTag |
  HierarchyTag |
  HistoryTag

data TreeNode x k v = TreeNode [(k, v)] x
data ListNode x r = Nil | Cons x r

data Data :: Tag -> * where
  JSONData :: JSON -> Data JSONTag
  HierarchyNode :: TreeNode (Ref JSONTag) Text (Ref HierarchyTag) -> Data HierarchyTag
  HistoryNode :: ListNode (MetaInfo, Ref HierarchyTag) (Ref HistoryTag) -> Data HistoryTag

type JSONData = Data JSONTag
type HierarchyNode = Data HierarchyTag
type HistoryNode = Data HistoryTag

data Ref :: Tag -> * where
  Ref :: ByteString -> Ref a

-- This defines the operations that are possible on the data in zookeeper
data StoreInstr a where
  Put :: Data t -> StoreInstr (Ref t)
  Get :: Ref t -> StoreInstr (Data t)
  GetHead :: StoreInstr (Ref HistoryTag)
  UpdateHead :: Ref HistoryTag -> Ref HistoryTag -> StoreInstr Bool

type StoreOp a = Program StoreInstr a

put :: Data x -> StoreOp (Ref x)
put = singleton . Put

get :: Ref x -> StoreOp (Data x)
get = singleton . Get

getHead :: StoreOp (Ref HistoryTag)
getHead = singleton GetHead

updateHead :: Ref HistoryTag -> Ref HistoryTag -> StoreOp Bool
updateHead prev next = singleton $ UpdateHead prev next

-- It should be possible to run a store operation
runStoreOp :: StoreOp a -> IO a
runStoreOp = undefined


main = print "hello world"
