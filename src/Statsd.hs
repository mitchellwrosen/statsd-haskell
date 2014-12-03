module Statsd
    (
    -- * Types
      StatsdT
    , Statsd
    , Bucket
    , SamplePct

    -- * StatsD API
    , runStatsd
    , statsdCounter
    , statsdSampledCounter
    , statsdTimer
    , statsdGauge
    , statsdGaugePlus
    , statsdGaugeMinus
    , statsdSet
    ) where

import           Control.Exception
import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.Writer
import           Control.Monad.Trans.Control
import           Data.ByteString           (ByteString)
import qualified Data.ByteString.Char8     as BS
import           Network
import           Network.Socket
import           Network.Socket.ByteString (sendAll)
import           System.Random             (randomRIO)

-- | The StatsdT monad transformer. Pushing to StatsD occurs in this monad, and
-- the computation is run with 'runStatsd'.
newtype StatsdT m a = StatsdT (ReaderT Socket (WriterT [ByteString] m) a)

-- | A simple type alias for pushing to StatsD in IO.
type Statsd a = StatsdT IO a

-- | A StatsD bucket.
type Bucket = ByteString

-- | Counter sample percent. Must be between 0.0 and 1.0, inclusive.
type SamplePct = Double

-- | Run a StatsdT computation, which pushes metrics to StatsD.
--
-- > {-# LANGUAGE OverloadedStrings #-}
-- > {-# LANGUAGE RecordWildCards   #-}
-- >
-- > module Main where
-- >
-- > import Network
-- > import Network.Socket
-- > import Statsd
-- >
-- > main :: IO ()
-- > main = do
-- >     let hints   = defaultHints
-- >                     { addrFamily     = AF_INET
-- >                     , addrSocketType = Datagram
-- >                     }
-- >         host    = "localhost"
-- >         service = "8125"
-- >
-- >     AddrInfo{..}:_ <- getAddrInfo (Just hints) (Just host) (Just service)
-- >     runStatsd addrFamily addrSocketType addrProtocol addrAddress $ do
-- >         statsdCounter "foo" 1
-- >         statsdTimer "bar" 25
runStatsd :: (MonadBaseControl IO m, MonadIO m)
          => Family
          -> SocketType
          -> ProtocolNumber
          -> SockAddr
          -> StatsdT m a -> m a
runStatsd family socket_type protocol_num sock_addr (StatsdT action) =
    withSocket family socket_type protocol_num sock_addr $ \sock -> do
        (a, bss) <- runWriterT (runReaderT action sock)
        liftIO $ sendAll sock (BS.intercalate "\n" bss)
        return a

-- | Push to a StatsD counter.
--
-- > statsdCounter "foo" 1 == "foo:1|c"
statsdCounter :: MonadIO m => Bucket -> Int -> StatsdT m ()
statsdCounter bucket n = StatsdT . lift . tell $ [encodeSimpleMetric bucket n "c"]

-- | Push to a StatsD counter, sampled.
--
-- > statsdSampledCounter "foo" 1 0.5 == "foo:1|c|@0.5"
statsdSampledCounter :: MonadIO m => Bucket -> Int -> SamplePct -> StatsdT m ()
statsdSampledCounter bucket n pct = StatsdT $ do
    r <- liftIO $ randomRIO (0.0, 1.0)
    when (r <= pct) $
        lift $ tell [encodeSimpleMetric bucket n "c" <> "|@" <> BS.pack (show pct)]

-- | Push to a StatsD timer.
--
-- > statsdTimer "foo" 1 == "foo:1|ms"
statsdTimer :: MonadIO m => Bucket -> Int -> StatsdT m ()
statsdTimer bucket n = StatsdT . lift . tell $ [encodeSimpleMetric bucket n "ms"]

-- | Push to a StatsD gauge.
--
-- > statsdGauge "foo" 1 == "foo:1|g"
statsdGauge :: MonadIO m => Bucket -> Int -> StatsdT m ()
statsdGauge bucket n = StatsdT . lift . tell $ [encodeSimpleMetric bucket n "g"]

-- | Push a positive delta to a StatsD gauge.
--
-- > statsdGaugePlus "foo" 1 == "foo:+1|g"
statsdGaugePlus :: MonadIO m => Bucket -> Int -> StatsdT m ()
statsdGaugePlus bucket n = StatsdT . lift . tell $ [bucket <> ":+" <> BS.pack (show n) <> "|g"]

-- | Push a negative delta to a StatsD gauge.
--
-- > statsdGaugePlus "foo" 1 == "foo:-1|g"
statsdGaugeMinus :: MonadIO m => Bucket -> Int -> StatsdT m ()
statsdGaugeMinus bucket n = StatsdT . lift . tell $ [bucket <> ":-" <> BS.pack (show n) <> "|g"]

-- | Push to a StatsD set.
--
-- > statsdGaugePlus "foo" 1 == "foo:1|s"
statsdSet :: MonadIO m => Bucket -> Int -> StatsdT m ()
statsdSet bucket n = StatsdT . lift . tell $ [encodeSimpleMetric bucket n "s"]

encodeSimpleMetric :: Bucket -> Int -> ByteString -> ByteString
encodeSimpleMetric bucket n typ = bucket <> ":" <> BS.pack (show n) <> "|" <> typ

withSocket :: MonadBaseControl IO m
           => Family
           -> SocketType
           -> ProtocolNumber
           -> SockAddr
           -> (Socket -> m a)
           -> m a
withSocket family socket_type protocol_num sock_addr = liftBaseOp (bracket acquire close)
  where
    acquire :: IO Socket
    acquire = do
        sock <- socket family socket_type protocol_num
        connect sock sock_addr
        return sock
