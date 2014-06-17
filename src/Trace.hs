module Trace where

import Debug.Trace (trace)

traceM :: (Show s, Monad m) => s -> m ()
traceM x = trace (show x) (return ())
