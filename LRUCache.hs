{-# LANGUAGE BangPatterns #-}
module LRUCache (
  newLRU
) where

import Prelude hiding (readList, last)

import Data.Hashable (Hashable)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap

import Control.Applicative ((<$>), (<*>), (<|>))
import Control.Monad (mapM, forM_, when)
import Control.Exception (try, SomeException, throw)
import Control.Concurrent.STM (STM, TVar, newTVar, readTVar, writeTVar, atomically, retry)
import Control.Concurrent.Async (Async, wait, async)

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
   else
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
  unlink node
  f <- first l
  link node Nothing f l

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

readList :: MList a -> Int -> STM [MNode a]
readList l n = do
  f <- first l
  readOnwards f n

readListValues :: MList a -> Int -> STM [a]
readListValues l n = readList l n >>= mapM getItem

readOnwards :: Maybe (MNode a) -> Int -> STM [(MNode a)]
readOnwards node n =
  readOnwards' node n []
 where
  readOnwards' :: Maybe (MNode a) -> Int -> [MNode a] -> STM [MNode a]
  readOnwards' Nothing _ xs = return xs
  readOnwards' _ 0 xs = return xs
  readOnwards' (Just x) n xs = do
    x' <- next (Just x)
    readOnwards' x' (n-1) (x:xs)

newtype TAsync a = TAsync (TVar (Maybe (Either (IO (Async a)) (Async a))))

newTAsync :: IO a -> STM (TAsync a)
newTAsync = fmap TAsync . newTVar . Just . Left . async

startTAsync :: TAsync a -> IO (Async a)
startTAsync (TAsync t) = do
  t' <- atomically $ do
    t' <- readTVar t >>= maybe retry return
    case t' of
      Left _ -> writeTVar t Nothing
      Right _ -> return ()
    return t'
  case t' of
    Left action -> do
      result <- action
      atomically $ writeTVar t (Just (Right result))
      return result
    Right result -> return result

waitTAsync :: TAsync a -> IO a
waitTAsync p = do
  x <- startTAsync p
  wait x

data LRU k v = LRU !(k -> IO v) !Int !(MList (k, TAsync v)) !(TVar (HashMap k (MNode (k, TAsync v))))

newLRU :: (Hashable k, Eq k) => (k -> IO v) -> Int -> IO (k -> IO v)
newLRU action n = readLRU <$> (atomically (LRU action n <$> newList <*> newTVar HashMap.empty))

readLRU :: (Hashable k, Eq k) => LRU k v -> k -> IO v
readLRU lru@(LRU action _ list m) key = do
  (b, t) <- atomically $ do
    m' <- readTVar m
    case HashMap.lookup key m' of
      Nothing -> do
        t <- newTAsync (action key)
        node <- newNode (key, t)
        pushFront node list
        writeTVar m (HashMap.insert key node m')
        popExcess lru
        return (True, t)
      Just node -> do
        pushFront node list
        ((,) False . snd) <$> getItem node
  if b then do
    x <- try $ waitTAsync t
    case x of
      Left err -> do
        atomically $ modifyTVar m (HashMap.delete key)
        throw (err :: SomeException)
      Right x' -> return x'
   else
    waitTAsync t
 where
  modifyTVar :: TVar a -> (a -> a) -> STM ()
  modifyTVar t f = do
    x <- readTVar t
    writeTVar t (f x)

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
