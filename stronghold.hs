{-# LANGUAGE GADTs, EmptyDataDecls, DataKinds, KindSignatures, OverloadedStrings, FlexibleInstances  #-}
module Main where

import Data.ByteString (ByteString)
import Data.Text (Text)
import qualified Data.Aeson as Aeson
import Data.Serialize

import Control.Applicative
import Control.Monad
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

instance Serialize Aeson.Value where
  put = put . Aeson.encode
  get = maybe (fail "error parsing json") return =<< Aeson.decode <$> get

instance Serialize (Ref t) where
  put (Ref x) = put x
  get = Ref <$> get

instance (Serialize k, Serialize v, Serialize x) => Serialize (TreeNode x k v) where
  put (TreeNode l x) = put (l, x)
  get = (\(l, x) -> TreeNode l x) <$> get

instance Serialize Text where
  put = undefined
  get = undefined

instance (Serialize x, Serialize r) => Serialize (ListNode x r) where
  put Nil = putByteString "n"
  put (Cons x xs) = do
    putByteString "c"
    put (x, xs)

  get = do
    b <- getBytes 1
    if b == "n" then
      return Nil
     else if b == "c" then
      (\(x, xs) -> Cons x xs) <$> get
     else
      fail "unknown listnode type"

-- FIXME
instance Serialize MetaInfo where
  put _ = return ()
  get = return $ MetaInfo () "" "" ()

instance Serialize (Data JSONTag) where
  put (JSONData json) = do
    putByteString "1"
    put json
  get = do
    t <- getBytes 1
    when (t /= "1") $ fail "expected json tag"
    JSONData <$> get

instance Serialize (Data HierarchyTag) where
  put (HierarchyNode x) = do
    putByteString "2"
    put x
  get = do
    t <- getBytes 1
    when (t /= "2") $ fail "expected hierarchy node tag"
    HierarchyNode <$> get

instance Serialize (Data HistoryTag) where
  put (HistoryNode x) = do
    putByteString "3"
    put x
  get = do
    t <- getBytes 1
    when (t /= "3") $ fail "expected history node tag"
    HistoryNode <$> get

type JSONData = Data JSONTag
type HierarchyNode = Data HierarchyTag
type HistoryNode = Data HistoryTag

data Ref :: Tag -> * where
  Ref :: ByteString -> Ref a

emptyObject :: Ref JSONTag -> Bool
emptyObject = undefined

-- This defines the operations that are possible on the data in zookeeper
data StoreInstr a where
  Store :: Data t -> StoreInstr (Ref t)
  Load :: Ref t -> StoreInstr (Data t)
  GetHead :: StoreInstr (Ref HistoryTag)
  UpdateHead :: Ref HistoryTag -> Ref HistoryTag -> StoreInstr Bool

type StoreOp a = Program StoreInstr a

store :: Data x -> StoreOp (Ref x)
store = singleton . Store

load :: Ref x -> StoreOp (Data x)
load = singleton . Load

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

storeJSON :: JSON' -> StoreOp (Ref JSONTag)
storeJSON (Left x) = return x
storeJSON (Right x) = store $ JSONData x

storeHierarchy :: Hierarchy -> StoreOp (Ref HierarchyTag)
storeHierarchy (Mu (Left x)) = return x
storeHierarchy (Mu (Right (TreeNode l json))) = do
  json' <- storeJSON json
  l' <- mapM (\(k, v) -> (,) k <$> storeHierarchy v) l
  store $ HierarchyNode (TreeNode l' json')

storeHistory :: History -> StoreOp (Ref HistoryTag)
storeHistory (Mu (Left x)) = return x
storeHistory (Mu (Right Nil)) = store $ HistoryNode Nil
storeHistory (Mu (Right (Cons (meta, hier) xs))) = do
  xs' <- storeHistory xs
  hier' <- storeHierarchy hier
  store $ HistoryNode $ Cons (meta, hier') xs'

emptyObject' :: JSON' -> Bool
emptyObject' (Left x) = emptyObject x
emptyObject' (Right x) = Aeson.object [] == x

refJSON :: JSON -> JSON'
refJSON = Right

derefJSON :: JSON' -> StoreOp JSON
derefJSON (Right x) = return x
derefJSON (Left r) = do
  JSONData d <- load r
  return d

refHistory :: ListNode (MetaInfo, Hierarchy) History -> History
refHistory = Mu . Right

derefHistory :: History -> StoreOp (ListNode (MetaInfo, Hierarchy) History)
derefHistory (Mu (Right x)) = return x
derefHistory (Mu (Left r)) = do
  HistoryNode l <- load r
  return $
    case l of
      Nil -> Nil
      Cons (meta, hierarchy) history -> Cons (meta, Mu (Left hierarchy)) (Mu (Left history))

refHierarchy :: TreeNode JSON' Text Hierarchy -> Hierarchy
refHierarchy = Mu . Right

derefHierarchy :: Hierarchy -> StoreOp (TreeNode JSON' Text Hierarchy)
derefHierarchy (Mu (Right x)) = return x
derefHierarchy (Mu (Left r)) = do
  HierarchyNode (TreeNode l json) <- load r
  return $ TreeNode (map (\(k, v) -> (k, Mu (Left v))) l) (Left json)

data HierarchyCtx = HierarchyCtx [(Text, [(Text, Hierarchy)], JSON')]
data HierarchyZipper = HierarchyZipper HierarchyCtx (TreeNode JSON' Text Hierarchy)

delete :: Eq k => k -> [(k, v)] -> [(k, v)]
delete _ [] = []
delete k1 ((k2, v):xs) =
  if k1 == k2 then
    xs
  else
    delete k1 xs

down :: Text -> HierarchyZipper -> StoreOp HierarchyZipper
down key (HierarchyZipper (HierarchyCtx hierCtx) (TreeNode children json)) =
  let hierCtx' = HierarchyCtx ((key, delete key children, json):hierCtx)
      def = return $ TreeNode [] (refJSON (Aeson.object [])) in
        HierarchyZipper hierCtx' <$> maybe def derefHierarchy (lookup key children)

up :: HierarchyZipper -> HierarchyZipper
up z@(HierarchyZipper (HierarchyCtx []) _) = z
up (HierarchyZipper (HierarchyCtx ((key, children', json'):xs)) hier@(TreeNode children json)) =
  HierarchyZipper (HierarchyCtx xs) $
    if null children && emptyObject' json then do
      (TreeNode children' json')
    else
      (TreeNode ((key, refHierarchy hier):children') json')

isTop :: HierarchyZipper -> Bool
isTop (HierarchyZipper (HierarchyCtx x) _) = null x

top :: HierarchyZipper -> HierarchyZipper
top z =
  if isTop z then
    z
  else
    top (up z)

loadJSON' :: HierarchyZipper -> JSON'
loadJSON' (HierarchyZipper _ (TreeNode _ json)) = json

loadJSON :: HierarchyZipper -> StoreOp (HierarchyZipper, JSON)
loadJSON (HierarchyZipper hierCtx (TreeNode children json')) = do
  json <- derefJSON json'
  return (HierarchyZipper hierCtx (TreeNode children (refJSON json)), json)

setJSON' :: JSON' -> HierarchyZipper -> HierarchyZipper
setJSON' json' (HierarchyZipper hierCtx (TreeNode children _)) =
  HierarchyZipper hierCtx (TreeNode children json')

children :: HierarchyZipper -> [Text]
children (HierarchyZipper _ (TreeNode children _)) = map fst children

solidifyHierarchy :: HierarchyZipper -> Hierarchy
solidifyHierarchy hier =
  let HierarchyZipper _ hier' = top hier in
    refHierarchy hier'

data HistoryCtx = HistoryCtx History [(MetaInfo, Hierarchy)]
data HistoryZipper = HistoryZipper HistoryCtx MetaInfo Hierarchy

back :: HistoryZipper -> StoreOp HistoryZipper
back (HistoryZipper (HistoryCtx l r) meta hier) = do
  l' <- derefHistory l
  return $ case l' of
    Nil -> HistoryZipper (HistoryCtx (refHistory l') r) meta hier
    Cons (meta', hier') xs ->
      HistoryZipper (HistoryCtx xs ((meta, hier):r)) meta' hier'

forward :: HistoryZipper -> HistoryZipper
forward z@(HistoryZipper (HistoryCtx l r) meta hier) = do
  case r of
    [] -> z
    ((meta', hier'):xs) ->
      HistoryZipper (HistoryCtx (refHistory (Cons (meta, hier) l)) xs) meta' hier'

isForwardMost :: HistoryZipper -> Bool
isForwardMost (HistoryZipper (HistoryCtx _ x) _ _) = null x

forwardMost :: HistoryZipper -> HistoryZipper
forwardMost z =
  if isForwardMost z then
    z
  else
    forwardMost (forward z)

loadMetaInfo :: HistoryZipper -> MetaInfo
loadMetaInfo (HistoryZipper _ meta _) = meta

setMetaInfo :: MetaInfo -> HistoryZipper -> HistoryZipper
setMetaInfo meta (HistoryZipper histCtx _ hier) =
  HistoryZipper histCtx meta hier

loadHierarchy :: HistoryZipper -> Hierarchy
loadHierarchy (HistoryZipper _ _ hier) = hier

append :: MetaInfo -> Hierarchy -> HistoryZipper -> HistoryZipper
append = undefined

solidifyHistory :: HistoryZipper -> History
solidifyHistory z =
  let (HistoryZipper (HistoryCtx l _) meta hier) = forwardMost z in
    refHistory (Cons (meta, hier) l)

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
