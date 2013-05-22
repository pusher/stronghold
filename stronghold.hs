{-# LANGUAGE GADTs, EmptyDataDecls, DataKinds, KindSignatures, OverloadedStrings, ScopedTypeVariables #-}
module Main where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Base16 as Base16
import qualified Data.ByteString.Char8 as BC
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import qualified Data.Aeson as Aeson
import Data.Serialize

import Control.Applicative
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Operational
import Control.Exception (tryJust)

import Snap.Core
import Snap.Http.Server

import Crypto.Hash.SHA1

import qualified Zookeeper as Zoo

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
  Ref :: ByteString -> Ref t

instance Eq (Ref t) where
  (Ref a) == (Ref b) = a == b

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
  put (MetaInfo a b c d) = put (a, b, c, d)
  get = (\(a, b, c, d) -> MetaInfo a b c d) <$> get

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

getZkPath :: ByteString -> String
getZkPath = ("/ref/" ++) . BC.unpack . Base16.encode

fetchRef :: Zoo.ZHandle -> ByteString -> IO (Maybe ByteString)
fetchRef zk ref = do
  (dat, _) <- Zoo.get zk (getZkPath ref) Zoo.NoWatch
  return dat

isRight :: Either a b -> Bool
isRight (Right _) = True
isRight _ = False

validHistoryNode :: ByteString -> Bool
validHistoryNode s = isRight (decode s :: Either String (Data HistoryTag))

isErrNodeExists :: Zoo.ZooError -> Bool
isErrNodeExists (Zoo.ErrNodeExists _) = True
isErrNodeExists _ = False

-- It should be possible to run a store operation
runStoreOp :: Zoo.ZHandle -> StoreOp a -> IO a
runStoreOp zk op =
  case view op of
    Return x -> return x
    (Store d) :>>= rest -> do
      let d' = encode d
      let h = hash d'
      tryJust (guard . isErrNodeExists) $ Zoo.create zk (getZkPath h) (Just d') Zoo.OpenAclUnsafe (Zoo.CreateMode False False)
      runStoreOp zk (rest (Ref h))
    (Load (Ref r)) :>>= rest -> do
      dat <- fetchRef zk r
      dat' <- maybe (fail "no such ref") return dat
      either fail (runStoreOp zk . rest) (decode dat')
    GetHead :>>= rest -> do
      (dat, _) <- Zoo.get zk "/head" Zoo.NoWatch
      head <- maybe (fail "no head") return dat
      runStoreOp zk (rest (Ref (fst (Base16.decode head))))
    (UpdateHead (Ref old) (Ref new)) :>>= rest -> do
      (dat, stat) <- Zoo.get zk "/head" Zoo.NoWatch
      dat' <- maybe (fail "no head") return dat
      if Base16.encode old == dat' then do
        -- FIXME: exceptions here should be caught
        result <- Zoo.set zk "/head" (Just (Base16.encode new)) (fromIntegral (Zoo.stat_version stat))
        runStoreOp zk (rest True)
       else
        runStoreOp zk (rest False)
    (CreateRef r) :>>= rest -> do
      let (r', _) = Base16.decode r
      dat <- fetchRef zk r'
      if maybe False validHistoryNode dat then
        runStoreOp zk (rest (Just (Ref r')))
       else
        runStoreOp zk (rest Nothing)

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

makeHierarchyZipper :: Hierarchy -> StoreOp HierarchyZipper
makeHierarchyZipper hier = HierarchyZipper (HierarchyCtx []) <$> derefHierarchy hier

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

followPath :: [Text] -> HierarchyZipper -> StoreOp HierarchyZipper
followPath = flip (foldM (flip down))

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
children (HierarchyZipper _ (TreeNode children _)) = map fst children

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
  refHierarchy (TreeNode [] (refJSON (Aeson.object [])))

lastHierarchy :: History -> StoreOp Hierarchy
lastHierarchy hist = do
  hist' <- derefHistory hist
  case hist' of
    Nil -> return emptyHierarchy
    Cons (_, hier) _ -> return hier

addHistory :: MetaInfo -> Hierarchy -> History -> History
addHistory meta hier hist = refHistory (Cons (meta, hier) hist)

site :: Zoo.ZHandle -> Snap ()
site zk =
  ifTop (writeBS "Stronghold say hi") <|>
  route [
    ("head", fetchHead),
    ("at/:timestamp", fetchTimestamp),
    (":version/", hasVersion)
   ]
 where
  hasVersion :: Snap ()
  hasVersion = do
    Just version <- getParam "version"
    ref <- liftIO $ runStoreOp zk (createRef version)
    case ref of
      Just ref' ->
        let hist = makeHistoryTree ref' in
          route [
            ("paths", paths hist),
            ("changes", changes hist),
            ("next", next hist),
            ("info", info hist),
            ("materialized/", materialized hist),
            ("peculiar/", peculiar hist),
            ("update/", update ref')
           ]
      Nothing -> writeBS "failed, invalid reference" -- TODO: fail properly

  fetchHead :: Snap ()
  fetchHead = ifTop $ method GET $ do
    (Ref head) <- liftIO $ runStoreOp zk getHead
    writeBS (Base16.encode head)

  fetchTimestamp :: Snap ()
  fetchTimestamp = ifTop $ method GET $ undefined

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

  changes :: History -> Snap ()
  changes ref = ifTop $ method GET $ undefined

  next :: History -> Snap ()
  next ref = ifTop $ method GET $ undefined

  info :: History -> Snap ()
  info ref = ifTop $ method GET $ undefined

  materialized :: History -> Snap ()
  materialized ref = method GET $ do
    path <- rqPathInfo <$> getRequest
    undefined

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

  update :: Ref HistoryTag -> Snap ()
  update ref = method POST $ do
    path <- rqPathInfo <$> getRequest
    let parts = Text.splitOn "/" (decodeUtf8 path)
    body <- readRequestBody 102400
    body' <- maybe (fail "couldn't parse body") return (Aeson.decode body)
    success <- liftIO $ runStoreOp zk $ do
      head <- getHead
      if ref == head then do
        let hist = makeHistoryTree ref
        hier <- lastHierarchy hist
        z <- makeHierarchyZipper hier
        z' <- followPath parts z
        let z'' = setJSON' (refJSON body') z'
        let hier' = solidifyHierarchyZipper z''
        let meta = MetaInfo () "" "" () -- FIXME
        let hist' = addHistory meta hier' hist
        ref' <- storeHistory hist'
        updateHead ref ref'
       else
        return False
    if success then
      -- send a 200, maybe even send the new version ref
      writeBS "OK"
     else
      -- TODO: send an error properly
      writeBS "ERR"

watcher :: Zoo.ZHandle -> Zoo.EventType -> Zoo.State -> String -> IO ()
watcher _zh zEventType zState path =
  putStrLn ("watch: '" ++ path ++ "' :: " ++ show zEventType ++ " " ++ show zState)

main :: IO ()
main = do
  let host_port = "localhost:2181"
  zk <- Zoo.init host_port (Just watcher) 10000 -- last param is the timeout
  quickHttpServe (site zk)
