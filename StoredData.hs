{-# LANGUAGE GADTs, DataKinds, KindSignatures, OverloadedStrings, ScopedTypeVariables #-}
module StoredData where

{-
  This module is responsible for logic for interacting with the data in
  zookeeper.
-}

import Data.Maybe (fromJust)
import Data.ByteString (ByteString)
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import qualified Data.Aeson as Aeson
import Data.Serialize (Serialize, decode, encode, get, put, getBytes, putByteString)
import Data.HashMap.Strict (HashMap, unionWith)
import qualified Data.HashMap.Strict as HashMap
import qualified Data.HashSet as HashSet
import Data.Hashable (Hashable)
import Data.Time.Clock (UTCTime (UTCTime), DiffTime)
import Data.Time.Calendar (Day (ModifiedJulianDay), toModifiedJulianDay)

import Control.Applicative ((<$>))
import Control.Monad (foldM, when)
import Control.Monad.Operational (ProgramViewT (..), singleton, view, Program)
import Control.Exception (tryJust)

import Crypto.Hash.SHA1 (hash)

import qualified ZkInterface as Zk

type JSON = Aeson.Value

data MetaInfo = MetaInfo UTCTime Text Text -- timestamp, comment, author

instance Aeson.ToJSON MetaInfo where
  toJSON (MetaInfo ts comment author) =
    Aeson.object [
      ("timestamp", Aeson.toJSON ts),
      ("comment", Aeson.String comment),
      ("author", Aeson.String author)
    ]

data Tag =
  JSONTag |
  HierarchyTag |
  HistoryTag

data STag :: Tag -> * where
  JSONTag' :: STag JSONTag
  HierarchyTag' :: STag HierarchyTag
  HistoryTag' :: STag HistoryTag

class TagClass t where
  tag :: STag t

instance TagClass JSONTag where
  tag = JSONTag'

instance TagClass HierarchyTag where
  tag = HierarchyTag'

instance TagClass HistoryTag where
  tag = HistoryTag'

data TreeNode x k v = TreeNode (HashMap k v) x
data ListNode x r = Nil | Cons x r

data Data :: Tag -> * where
  JSONData :: JSON -> Data JSONTag
  HierarchyNode :: TreeNode (Ref JSONTag) Text (Ref HierarchyTag) -> Data HierarchyTag
  HistoryNode :: ListNode (MetaInfo, Ref HierarchyTag) (Ref HistoryTag) -> Data HistoryTag

type JSONData = Data JSONTag
type HierarchyNode = Data HierarchyTag
type HistoryNode = Data HistoryTag

data Ref :: Tag -> * where
  Ref :: ByteString -> Ref t

instance Eq (Ref t) where
  (Ref a) == (Ref b) = a == b

instance Serialize UTCTime where 
    get = uncurry UTCTime <$> get
    put (UTCTime day time) = put (day, time)

instance Serialize Day where
    get = ModifiedJulianDay <$> get
    put = put . toModifiedJulianDay

instance Serialize DiffTime where 
    get = fromRational <$> get
    put = put . toRational

instance Serialize Aeson.Value where
  put = put . Aeson.encode
  get = maybe (fail "error parsing json") return =<< Aeson.decode <$> get

instance Serialize (Ref t) where
  put (Ref x) = put x
  get = Ref <$> get

instance (Eq k, Hashable k, Serialize k, Serialize v) => Serialize (HashMap k v) where
  put = put . HashMap.toList
  get = HashMap.fromList <$> get

instance (Eq k, Hashable k, Serialize k, Serialize v, Serialize x) => Serialize (TreeNode x k v) where
  put (TreeNode l x) = put (l, x)
  get = (\(l, x) -> TreeNode l x) <$> get

instance Serialize Text where
  put = put . encodeUtf8
  get = decodeUtf8 <$> get

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

instance Serialize MetaInfo where
  put (MetaInfo a b c) = put (a, b, c)
  get = (\(a, b, c) -> MetaInfo a b c) <$> get

instance TagClass t => Serialize (Data t) where
  put (JSONData json) = do
    putByteString "1"
    put json
  put (HierarchyNode x) = do
    putByteString "2"
    put x
  put (HistoryNode x) = do
    putByteString "3"
    put x

  get = do
    t <- getBytes 1
    case (tag :: STag t) of
      JSONTag' -> do
        when (t /= "1") $ fail "expected json tag"
        JSONData <$> get
      HierarchyTag' -> do
        when (t /= "2") $ fail "expected hierarchy node tag"
        HierarchyNode <$> get
      HistoryTag' -> do
        when (t /= "3") $ fail "expected history node tag"
        HistoryNode <$> get

emptyObjectHash :: ByteString
emptyObjectHash = hash (encode (JSONData (Aeson.object [])))

-- Arguably, this should be inside the monad.
emptyObject :: Ref JSONTag -> Bool
emptyObject (Ref x) = x == emptyObjectHash

-- This defines the operations that are possible on the data in zookeeper
data StoreInstr a where
  Store :: TagClass t => Data t -> StoreInstr (Ref t)
  Load :: TagClass t => Ref t -> StoreInstr (Data t)
  GetHead :: StoreInstr (Ref HistoryTag)
  UpdateHead :: Ref HistoryTag -> Ref HistoryTag -> StoreInstr Bool
  CreateRef :: ByteString -> StoreInstr (Maybe (Ref HistoryTag))

type StoreOp a = Program StoreInstr a

store :: TagClass x => Data x -> StoreOp (Ref x)
store = singleton . Store

load :: TagClass x => Ref x -> StoreOp (Data x)
load = singleton . Load

getHead :: StoreOp (Ref HistoryTag)
getHead = singleton GetHead

updateHead :: Ref HistoryTag -> Ref HistoryTag -> StoreOp Bool
updateHead prev next = singleton $ UpdateHead prev next

createRef :: ByteString -> StoreOp (Maybe (Ref HistoryTag))
createRef = singleton . CreateRef

isRight :: Either a b -> Bool
isRight (Right _) = True
isRight _ = False

validHistoryNode :: ByteString -> Bool
validHistoryNode s = isRight (decode s :: Either String (Data HistoryTag))

-- It should be possible to run a store operation
runStoreOp :: Zk.ZkInterface -> StoreOp a -> IO a
runStoreOp zk op =
  case view op of
    Return x -> return x
    (Store d) :>>= rest -> do
      let d' = encode d
      ref <- Zk.storeData zk d'
      ref' <- maybe (fail "couldn't store node in zookeeper") return ref
      runStoreOp zk (rest (Ref ref'))
    (Load (Ref r)) :>>= rest -> do
      dat <- Zk.loadData zk r
      dat' <- maybe (fail "no such ref") return dat
      either fail (runStoreOp zk . rest) (decode dat')
    GetHead :>>= rest -> do
      head <- Zk.getHead zk
      head' <- maybe (fail "couldn't fetch head") return head
      runStoreOp zk (rest (Ref head'))
    (UpdateHead (Ref old) (Ref new)) :>>= rest -> do
      b <- Zk.updateHead zk old new
      runStoreOp zk (rest b)
    (CreateRef r) :>>= rest -> do
      b <- Zk.hasReference zk r
      if b then
        runStoreOp zk (rest (Just (Ref r)))
       else
        runStoreOp zk (rest (Nothing))

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

makeHistoryTree :: Ref HistoryTag -> History
makeHistoryTree = Mu . Left

storeJSON :: JSON' -> StoreOp (Ref JSONTag)
storeJSON (Left x) = return x
storeJSON (Right x) = store $ JSONData x

storeHierarchy :: Hierarchy -> StoreOp (Ref HierarchyTag)
storeHierarchy (Mu (Left x)) = return x
storeHierarchy (Mu (Right (TreeNode l json))) = do
  json' <- storeJSON json
  l' <- HashMap.fromList <$> mapM (\(k, v) -> (,) k <$> storeHierarchy v) (HashMap.toList l)
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
  return $ TreeNode (HashMap.map (\v -> Mu (Left v)) l) (Left json)

data HierarchyCtx = HierarchyCtx [(Text, HashMap Text Hierarchy, JSON')]
data HierarchyZipper = HierarchyZipper HierarchyCtx (TreeNode JSON' Text Hierarchy)

makeHierarchyZipper :: Hierarchy -> StoreOp HierarchyZipper
makeHierarchyZipper hier = HierarchyZipper (HierarchyCtx []) <$> derefHierarchy hier

down :: Text -> HierarchyZipper -> StoreOp HierarchyZipper
down key (HierarchyZipper (HierarchyCtx hierCtx) (TreeNode children json)) =
  let hierCtx' = HierarchyCtx ((key, HashMap.delete key children, json):hierCtx)
      def = return $ TreeNode HashMap.empty (refJSON (Aeson.object [])) in
        HierarchyZipper hierCtx' <$> maybe def derefHierarchy (HashMap.lookup key children)

followPath :: [Text] -> HierarchyZipper -> StoreOp HierarchyZipper
followPath = flip (foldM (flip down))

up :: HierarchyZipper -> HierarchyZipper
up z@(HierarchyZipper (HierarchyCtx []) _) = z
up (HierarchyZipper (HierarchyCtx ((key, children', json'):xs)) hier@(TreeNode children json)) =
  HierarchyZipper (HierarchyCtx xs) $
    if HashMap.null children && emptyObject' json then do
      (TreeNode children' json')
    else
      (TreeNode (HashMap.insert key (refHierarchy hier) children') json')

isTop :: HierarchyZipper -> Bool
isTop (HierarchyZipper (HierarchyCtx x) _) = null x

top :: HierarchyZipper -> HierarchyZipper
top z =
  if isTop z then
    z
  else
    top (up z)

getJSON' :: HierarchyZipper -> JSON'
getJSON' (HierarchyZipper _ (TreeNode _ json)) = json

getJSON :: HierarchyZipper -> StoreOp (HierarchyZipper, JSON)
getJSON (HierarchyZipper hierCtx (TreeNode children json')) = do
  json <- derefJSON json'
  return (HierarchyZipper hierCtx (TreeNode children (refJSON json)), json)

setJSON' :: JSON' -> HierarchyZipper -> HierarchyZipper
setJSON' json' (HierarchyZipper hierCtx (TreeNode children _)) =
  HierarchyZipper hierCtx (TreeNode children json')

children :: HierarchyZipper -> [Text]
children (HierarchyZipper _ (TreeNode children _)) = HashMap.keys children

solidifyHierarchyZipper :: HierarchyZipper -> Hierarchy
solidifyHierarchyZipper hier =
  let HierarchyZipper _ hier' = top hier in
    refHierarchy hier'

subPaths :: HierarchyZipper -> StoreOp [Text]
subPaths z =
  ([""] ++) <$> concat <$> mapM (\child -> do
    z' <- down child z
    paths <- subPaths z'
    return (map (\i -> Text.concat ["/", child, i]) paths)) (children z)

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

getMetaInfo :: HistoryZipper -> MetaInfo
getMetaInfo (HistoryZipper _ meta _) = meta

setMetaInfo :: MetaInfo -> HistoryZipper -> HistoryZipper
setMetaInfo meta (HistoryZipper histCtx _ hier) =
  HistoryZipper histCtx meta hier

getHierarchy :: HistoryZipper -> Hierarchy
getHierarchy (HistoryZipper _ _ hier) = hier

solidifyHistory :: HistoryZipper -> History
solidifyHistory z =
  let (HistoryZipper (HistoryCtx l _) meta hier) = forwardMost z in
    refHistory (Cons (meta, hier) l)

emptyHierarchy :: Hierarchy
emptyHierarchy =
  refHierarchy (TreeNode HashMap.empty (refJSON (Aeson.object [])))

lastHierarchy :: History -> StoreOp Hierarchy
lastHierarchy hist = do
  hist' <- derefHistory hist
  case hist' of
    Nil -> return emptyHierarchy
    Cons (_, hier) _ -> return hier

lastMetaInfo :: History -> StoreOp (Maybe MetaInfo)
lastMetaInfo hist = do
  hist' <- derefHistory hist
  case hist' of
    Nil -> return Nothing
    Cons (meta, _) _ -> return (Just meta)

addHistory :: MetaInfo -> Hierarchy -> History -> History
addHistory meta hier hist = refHistory (Cons (meta, hier) hist)

deepMerge :: JSON -> JSON -> JSON
deepMerge (Aeson.Object a) (Aeson.Object b) = Aeson.Object $ unionWith deepMerge a b
deepMerge _ x = x

updateHierarchy :: MetaInfo -> [Text] -> JSON -> Ref HistoryTag -> StoreOp (Maybe (Ref HistoryTag))
updateHierarchy meta parts json ref = do
  head <- getHead
  if head == ref then do
    (hist, headZipper) <- hierarchyZipperFromHistoryRef head
    hist' <- makeUpdate hist headZipper
    result <- updateHead head hist'
    if result then
      return (Just hist')
     else do
      head <- getHead
      (_, refZipper) <- hierarchyZipperFromHistoryRef ref
      attemptUpdate head refZipper
   else do
    (_, refZipper) <- hierarchyZipperFromHistoryRef ref
    attemptUpdate head refZipper
 where
  attemptUpdate :: Ref HistoryTag -> HierarchyZipper -> StoreOp (Maybe (Ref HistoryTag))
  attemptUpdate head refZipper = do
    (hist, headZipper) <- hierarchyZipperFromHistoryRef head
    if comparePaths headZipper refZipper then do
      hist' <- makeUpdate hist headZipper
      result <- updateHead head hist'
      if result then
        return (Just hist')
       else do
        head <- getHead
        attemptUpdate head refZipper
     else
      return Nothing

  makeUpdate :: History -> HierarchyZipper -> StoreOp (Ref HistoryTag)
  makeUpdate hist headZipper = do
    let headZipper' = setJSON' (refJSON json) headZipper
    let hier = solidifyHierarchyZipper headZipper'
    let hist' = addHistory meta hier hist
    storeHistory hist'

  hierarchyZipperFromHistoryRef :: Ref HistoryTag -> StoreOp (History, HierarchyZipper)
  hierarchyZipperFromHistoryRef ref = do
    let hist = makeHistoryTree ref
    hier <- lastHierarchy hist
    z <- makeHierarchyZipper hier
    z' <- followPath parts z
    return (hist, z')

  -- This is comparing two HierarchyZippers that are expected to be the result 
  -- of "followPath path" on a reference.
  -- This comparison is specific to updateHierarchy, think twice before using 
  -- it more generally.
  comparePaths :: HierarchyZipper -> HierarchyZipper -> Bool
  comparePaths (HierarchyZipper (HierarchyCtx ctxa) hiera) (HierarchyZipper (HierarchyCtx ctxb) hierb) =
    compareTreeNodes hiera hierb &&
    (not $ any (\((namea, _, jsona), (nameb, _, jsonb)) ->
      not (namea == nameb && jsona == jsonb)) $ zip ctxa ctxb)

  compareTreeNodes :: TreeNode JSON' Text Hierarchy -> TreeNode JSON' Text Hierarchy -> Bool
  compareTreeNodes (TreeNode itemsa jsona) (TreeNode itemsb jsonb) =
    jsona == jsonb &&
    compareHashMaps compareHierarchies itemsa itemsb

  compareHierarchies :: Hierarchy -> Hierarchy -> Bool
  compareHierarchies (Mu (Left a)) (Mu (Left b)) = a == b
  compareHierarchies _ _ = False

  compareHashMaps :: (Eq k, Hashable k) => (a -> b -> Bool) -> HashMap k a -> HashMap k b -> Bool
  compareHashMaps f a b =
    HashSet.fromList (HashMap.keys a) == HashSet.fromList (HashMap.keys b) &&
    (not $ any (\(k, v) -> not $ f v (fromJust (HashMap.lookup k b))) (HashMap.toList a))
