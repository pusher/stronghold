{-# LANGUAGE BangPatterns #-}
module LRUCache (
  newLRU
) where

import Prelude hiding (readList, last)

import Data.Hashable (Hashable)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap

import Control.Applicative ((<$>), (<*>), (<|>))
import Control.Concurrent.STM (STM, TVar, newTVar, readTVar, writeTVar, atomically)
import Control.Monad (mapM, forM_, when)
import Control.Exception (try, SomeException, throw)

newtype MList a =
  MList (TVar (Maybe (MNode a, MNode a), Int))
  deriving Eq

newtype MNode a =
  MNode (TVar (a, Maybe (MNode a), Maybe (MNode a), Maybe (MList a)))
  deriving Eq

newList :: STM (MList a)
newList = MList <$> newTVar (Nothing, 0)

newNode :: a -> STM (MNode a)
newNode x = MNode <$> newTVar (x, Nothing, Nothing, Nothing)

prev :: Maybe (MNode a) -> STM (Maybe (MNode a))
prev Nothing = return Nothing
prev (Just (MNode node)) = do
  (_, p, _, _) <- readTVar node
  return p

next :: Maybe (MNode a) -> STM (Maybe (MNode a))
next Nothing = return Nothing
next (Just (MNode node)) = do
  (_, _, n, _) <- readTVar node
  return n

getItem :: MNode a -> STM a
getItem (MNode node) = do
  (x, _, _, _) <- readTVar node
  return x

setNext :: Maybe (MNode a) -> Maybe (MNode a) -> STM ()
setNext Nothing _ = return ()
setNext (Just (MNode node)) n = do
  (a, p, _, l) <- readTVar node
  writeTVar node (a, p, n, l)

setPrev :: Maybe (MNode a) -> Maybe (MNode a) -> STM ()
setPrev Nothing _ = return ()
setPrev (Just (MNode node)) p = do
  (a, _, n, l) <- readTVar node
  writeTVar node (a, p, n, l)

areNeighbours :: Maybe (MNode a) -> Maybe (MNode a) -> STM Bool
areNeighbours Nothing Nothing = return True
areNeighbours Nothing x = do
  x' <- prev x
  return (x' == Nothing)
areNeighbours x Nothing = do
  x' <- next x
  return (x' == Nothing)
areNeighbours p n = do
  n' <- next p
  p' <- next n
  return ((n' == n) && (p' == p))

link :: MNode a -> Maybe (MNode a) -> Maybe (MNode a) -> MList a -> STM ()
link node@(MNode node') p n list@(MList list') = do
  unlink node
  (x, _, _, _) <- readTVar node'
  b <- areNeighbours p n
  if b then do
    setNext p (Just node)
    setPrev n (Just node)
    (y, !c) <- readTVar list'
    let y' = case y of
              Nothing -> (node, node)
              Just (first, last) ->
                let first' = if Just first == n then node else first
                    last' = if Just last == p then node else last in
                      (first', last')
    writeTVar list' (Just y', c+1)
    writeTVar node' (x, p, n, Just list)
   else do
    fail "can only link neighbouring nodes"

unlink :: MNode a -> STM ()
unlink node@(MNode node') = do
  (x, p, n, list) <- readTVar node'
  setNext p n
  setPrev n p
  case list of
    Nothing -> return ()
    Just (MList list') -> do
      (y, !c) <- readTVar list'
      case y of
        Nothing -> return ()
        Just (f, l) ->
          let f' = if f == node then n else Just f
              l' = if l == node then p else Just l in
                writeTVar list' ((,) <$> f' <*> l', c-1)
  writeTVar node' (x, Nothing, Nothing, Nothing)

first :: MList a -> STM (Maybe (MNode a))
first (MList l) = (fmap fst . fst) <$> readTVar l

last :: MList a -> STM (Maybe (MNode a))
last (MList l) = (fmap snd . fst) <$> readTVar l

pushFront :: MNode a -> MList a -> STM ()
pushFront node l = do
  f <- first l
  link node Nothing f l

pushBack :: MNode a -> MList a -> STM ()
pushBack node l = do
  b <- last l
  link node b Nothing l

popFront :: MList a -> STM (Maybe (MNode a))
popFront x = do
  f <- first x
  maybe (return ()) unlink f
  return f

popBack :: MList a -> STM (Maybe (MNode a))
popBack x = do
  l <- last x
  maybe (return ()) unlink l
  return l

size :: MList a -> STM Int
size (MList l) = snd <$> readTVar l

readList :: MList a -> STM [MNode a]
readList l = do
  f <- first l
  readOnwards f

readListValues :: MList a -> STM [a]
readListValues l = readList l >>= mapM getItem

readOnwards :: Maybe (MNode a) -> STM [(MNode a)]
readOnwards Nothing = return []
readOnwards (Just x) = do
  x' <- next (Just x)
  xs <- readOnwards x'
  return (x:xs)

data LRU k v = LRU !(k -> IO v) !Int !(MList (k, v)) !(TVar (HashMap k (MNode (k, v))))

newLRU :: (Hashable k, Eq k) => (k -> IO v) -> Int -> IO (k -> IO v)
newLRU action n = readLRU <$> (atomically (LRU action n <$> newList <*> newTVar HashMap.empty))

readLRU :: (Hashable k, Eq k) => LRU k v -> k -> IO v
readLRU lru@(LRU action _ list map) key = do
  map' <- atomically (readTVar map)
  case HashMap.lookup key map' of
    Nothing -> do
      value <- try $ action key
      case value of
        Left err -> throw (err :: SomeException)
        Right value' ->
          atomically $ do
            node <- newNode (key, value')
            pushFront node list
            writeTVar map (HashMap.insert key node map')
            popExcess lru
            return value'
    Just node -> do
      value <- snd <$> atomically (getItem node)
      atomically $ pushFront node list
      return value
 where
  modifyTVar :: TVar a -> (a -> a) -> STM ()
  modifyTVar v f = do
    v' <- readTVar v
    writeTVar v (f v')

  popExcess :: (Hashable k, Eq k) => LRU k v -> STM ()
  popExcess lru@(LRU _ n list map) = do
    m <- size list
    when (m > n) $ do
      node <- popBack list
      case node of
        Nothing -> return ()
        Just node' -> do
          key <- fst <$> getItem node'
          modifyTVar map (HashMap.delete key)
          popExcess lru
