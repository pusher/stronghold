import LRUCache
import Control.Monad

d = [1..1000000]
d' :: [Int]
d' = map (\x -> x `mod` 20) d

main = do
  lru <- newLRU (const (return 1)) 10
  forM_ d' lru
