import Control.Monad (const)

import qualified LRUCache as L

d = [1..1000000]
d' :: [Int]
d' = map (\x -> x `mod` 20) d

main = do
  lru <- L.newLRU (const (return 1)) 10
  forM_ d' lru
