{-# LANGUAGE GADTs, DataKinds, KindSignatures, OverloadedStrings, ScopedTypeVariables #-}
module StoredData where

{-
  This module is responsible for defining the data that get stored in
  zookeeper.
-}

import Data.ByteString (ByteString)
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import qualified Data.Aeson as Aeson
import Data.Serialize (Serialize, decode, encode, get, put, getBytes, putByteString)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.Hashable (Hashable)
import Data.Time.Clock (UTCTime (UTCTime), DiffTime)
import Data.Time.Calendar (Day (ModifiedJulianDay), toModifiedJulianDay)

import Control.Applicative ((<$>))
import Control.Monad (when)
import Control.Monad.Operational (ProgramViewT (..), singleton, view, Program)

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
