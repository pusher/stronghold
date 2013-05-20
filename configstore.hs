{-# LANGUAGE GADTs, EmptyDataDecls, DataKinds, KindSignatures, OverloadedStrings #-}
module Main where

import Data.ByteString (ByteString)
import Data.Text (Text)
import qualified Data.Aeson as Aeson

import Control.Applicative
import Control.Monad.Operational

import Snap.Core
import Snap.Util.FileServe
import Snap.Http.Server

type JSON = Aeson.Value

type Comment = Text
type Author = Text
type ChangeSet = ()
type Timestamp = ()

data MetaInfo = MetaInfo Timestamp Comment Author ChangeSet

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

-- This is a little tricky, if you're having difficulty understanding, read up 
-- on fixed point combinators and fixed point types.

-- Essentially, what's going on here is that a tree is being defined by taking 
-- the fixed point of (m :: * -> *). Except that, at every recursion site, 
-- there might be a reference instead of a subtree.
newtype Mu t m = Mu (Either t (m (Mu t m)))

-- These are data types that may have pieces that aren't in local memory.
type JSON' = Either (Ref JSONTag) JSON
type Hierarchy = Mu (Ref HierarchyTag) (TreeNode JSON' Text)
type History = Mu (Ref HistoryTag) (ListNode (MetaInfo, Hierarchy))

refJSON :: JSON -> JSON'
refJSON = Right

derefJSON :: JSON' -> StoreOp JSON
derefJSON (Right x) = return x
derefJSON (Left r) = do
  JSONData d <- get r
  return d

refHistory :: ListNode (MetaInfo, Hierarchy) History -> History
refHistory = Mu . Right

derefHistory :: History -> StoreOp (ListNode (MetaInfo, Hierarchy) History)
derefHistory (Mu (Right x)) = return x
derefHistory (Mu (Left r)) = do
  HistoryNode l <- get r
  return $
    case l of
      Nil -> Nil
      Cons (meta, hierarchy) history -> Cons (meta, Mu (Left hierarchy)) (Mu (Left history))

refHierarchy :: TreeNode JSON' Text Hierarchy -> Hierarchy
refHierarchy = Mu . Right

derefHierarchy :: Hierarchy -> StoreOp (TreeNode JSON' Text Hierarchy)
derefHierarchy (Mu (Right x)) = return x
derefHierarchy (Mu (Left r)) = do
  HierarchyNode (TreeNode l json) <- get r
  return $ TreeNode (map (\(k, v) -> (k, Mu (Left v))) l) (Left json)

-- These types specify data structures with holes.
data HistoryCtx = HistoryCtx History [(MetaInfo, Hierarchy)]
data HierarchyCtx = HierarchyCtx [([Hierarchy], [Hierarchy], JSON')]

data Zipper = Zipper HistoryCtx MetaInfo HierarchyCtx Hierarchy

site :: Snap ()
site =
    ifTop (writeBS "hello world") <|>
    route [ ("foo", writeBS "bar")
          , ("echo/:echoparam", echoHandler)
          ] <|>
    dir "static" (serveDirectory ".")

echoHandler :: Snap ()
echoHandler = do
    param <- getParam "echoparam"
    maybe (writeBS "must specify echo/param in URL")
          writeBS param

main :: IO ()
main = quickHttpServe site

