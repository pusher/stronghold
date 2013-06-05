{-# LANGUAGE GADTs, DataKinds, OverloadedStrings #-}
module Trees where

{-
  This module is responsible for the interacting with the tree structure on the
  data in zookeeper.
-}

import Data.Maybe (fromJust)
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Aeson as Aeson
import Data.HashMap.Strict (HashMap, unionWith)
import qualified Data.HashMap.Strict as HashMap
import qualified Data.HashSet as HashSet
import Data.Hashable (Hashable)

import Control.Applicative ((<$>))
import Control.Monad (foldM)

import StoredData

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

back :: HistoryZipper -> StoreOp (Maybe HistoryZipper)
back (HistoryZipper (HistoryCtx l r) meta hier) = do
  l' <- derefHistory l
  return $ case l' of
    Nil -> Nothing
    Cons (meta', hier') xs ->
      Just (HistoryZipper (HistoryCtx xs ((meta, hier):r)) meta' hier')

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

-- from < x <= to
revisionsBetween :: Ref HistoryTag -> Ref HistoryTag -> StoreOp (Maybe [Ref HistoryTag])
revisionsBetween from to =
  fmap (fmap reverse) (revisionsBetween from to)
 where
  revisionsBetween' :: Ref HistoryTag -> Ref HistoryTag -> StoreOp (Maybe [Ref HistoryTag])
  revisionsBetween' from to = do
    hist <- derefHistory (makeHistoryTree to)
    case hist of
      Nil -> return Nothing
      Cons x (Mu (Left xs)) -> do
        revisions <- revisionsBetween' from xs
        return (fmap (to :) revisions)

hierarchyFromRevision :: Ref HistoryTag -> StoreOp Hierarchy
hierarchyFromRevision = lastHierarchy . makeHistoryTree

materializedView :: [Text] -> Hierarchy -> StoreOp (Hierarchy, JSON)
materializedView path hier = do
  z <- makeHierarchyZipper hier
  (z', json) <- getJSON z
  (z'', jsons) <- foldM (\(z, l) label -> do
    z' <- down label z
    (z'', json) <- getJSON z'
    return (z'', json:l)) (z', [json]) path
  return (solidifyHierarchyZipper z'', foldl1 deepMerge $ reverse jsons)

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
