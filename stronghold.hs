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
data HistoryCtx = HistoryCtx History [(MetaInfo, TreeNode JSON' Text Hierarchy)]
data HierarchyCtx = HierarchyCtx [(Text, [(Text, Hierarchy)], JSON')]

data Zipper = Zipper HistoryCtx MetaInfo HierarchyCtx (TreeNode JSON' Text Hierarchy)

delete :: Eq k => k -> [(k, v)] -> [(k, v)]
delete _ [] = []
delete k1 ((k2, v):xs) =
  if k1 == k2 then
    xs
  else
    delete k1 xs

down :: Text -> Zipper -> StoreOp Zipper
down key (Zipper histCtx meta (HierarchyCtx hierCtx) (TreeNode children json)) =
  let hierCtx' = HierarchyCtx ((key, delete key children, json):hierCtx)
      def = return $ TreeNode [] (refJSON (Aeson.object [])) in
        Zipper histCtx meta hierCtx' <$> maybe def derefHierarchy (lookup key children)

up :: Zipper -> StoreOp Zipper
up (Zipper histCtx meta (HierarchyCtx []) hier) =
  return $ Zipper histCtx meta (HierarchyCtx []) hier
up (Zipper histCtx meta (HierarchyCtx ((key, children', json'):xs)) hier@(TreeNode children json)) =
  if null children then do
    json'' <- derefJSON json
    if json'' == Aeson.object [] then
      return $ Zipper histCtx meta (HierarchyCtx xs) (TreeNode children' json')
     else
      construct (refJSON json'')
  else
    construct json
 where
  construct json' =
    return $ Zipper histCtx meta (HierarchyCtx xs) (TreeNode ((key, refHierarchy hier):children) json')

isTop :: Zipper -> Bool
isTop (Zipper _ _ (HierarchyCtx x) _ ) = null x

top :: Zipper -> StoreOp Zipper
top z =
  if isTop z then
    return z
  else
    up z >>= top

getJSON' :: Zipper -> JSON'
getJSON' (Zipper _ _ _ (TreeNode _ json)) = json

getJSON :: Zipper -> StoreOp (Zipper, JSON)
getJSON (Zipper histCtx meta hierCtx (TreeNode children json')) = do
  json <- derefJSON json'
  return (Zipper histCtx meta hierCtx (TreeNode children (refJSON json)), json)

setJSON' :: JSON' -> Zipper -> Zipper
setJSON' json' (Zipper histCtx meta hierCtx (TreeNode children _)) =
  Zipper histCtx meta hierCtx (TreeNode children json')

children :: Zipper -> [Text]
children (Zipper _ _ _ (TreeNode children _)) = map fst children

left :: Zipper -> StoreOp Zipper
left z = do
  Zipper (HistoryCtx l r) meta hierCtx hier <- top z
  l' <- derefHistory l
  case l' of
    Nil -> return $ Zipper (HistoryCtx (refHistory l') r) meta hierCtx hier
    Cons (meta', hier') xs -> do
      hier'' <- derefHierarchy hier'
      return $ Zipper (HistoryCtx xs ((meta, hier):r)) meta' (HierarchyCtx []) hier''

right :: Zipper -> StoreOp Zipper
right z = do
  z'@(Zipper (HistoryCtx l r) meta hierCtx hier) <- top z
  case r of
    [] -> return z'
    ((meta', hier'):xs) ->
      return $ Zipper (HistoryCtx (refHistory (Cons (meta, refHierarchy hier) l)) xs) meta' (HierarchyCtx []) hier'

getMetaInfo :: Zipper -> MetaInfo
getMetaInfo (Zipper _ meta _ _) = meta

setMetaInfo :: MetaInfo -> Zipper -> Zipper
setMetaInfo meta (Zipper histCtx _ hierCtx hier) =
  Zipper histCtx meta hierCtx hier

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
