{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
module Main where

import Lib
import Data.Text (Text)
import Control.Lens
import Control.Concurrent (threadDelay)
import Control.Monad
import Control.Monad.Reader
import Control.Monad.Trans.Resource
import Control.Monad.Trans.AWS
import Control.Monad.IO.Class
import Data.Monoid
import System.IO
import Control.Concurrent.Async
import qualified Data.HashMap.Strict as HM
--import Network.AWS
import Network.AWS.DynamoDB

type AWS = AWST (ResourceT IO)

tableName = "estest1"

forkChildAws :: String -> AWS () -> AWS ()
forkChildAws childThreadName c = do
  runtimeEnv <- ask
  _ <- lift $ allocate (Control.Concurrent.Async.async (runResourceT $ runAWST runtimeEnv c)) onDispose
  return ()
  where
    onDispose a = do
      putStrLn ("disposing" <> childThreadName)
      Control.Concurrent.Async.cancel a

readFromDynamoAws1 :: Text -> Text -> AWS ()
readFromDynamoAws1 streamId eventNumber = do
    let key = HM.fromList
                [ ("streamId", set avS (Just streamId) attributeValue)
                , ("eventNumber", set avN (Just (eventNumber)) attributeValue)]
    let req = getItem tableName & set giKey key & set giConsistentRead (Just True)
    resp <- send req
    return ()

itemAttribute :: Text
              -> Lens' AttributeValue (Maybe v)
              -> v
              -> (Text, AttributeValue)
itemAttribute key l value = (key, set l (Just value) attributeValue)

updateItemAws1 :: Text -> Text -> AWS ()
updateItemAws1 streamId eventNumber = 
    go
  where
    go = do
        let key = 
                HM.fromList
                    [ itemAttribute "streamId" avS streamId
                    , itemAttribute "eventNumber" avN eventNumber]
        let expressionAttributeValues = 
                HM.singleton ":PageStatus" (set avS (Just "SomeValue222") attributeValue)
        let updateExpression = "SET PageStatus= :PageStatus"
        let req0 = 
                Network.AWS.DynamoDB.updateItem tableName & set uiKey key &
                set uiUpdateExpression (Just updateExpression) &
                if (not . HM.null) expressionAttributeValues
                    then set
                             uiExpressionAttributeValues
                             expressionAttributeValues
                    else id
        _ <- send req0
        return ()

testScanAws :: AWS ()
testScanAws = forever $ do
  result <- readFromDynamoAws1 "page$00000000" "0"
  void $ updateItemAws1 "page$00000000" "0"
  liftIO $ threadDelay 100000

replicateProblemAws :: AWS ()
replicateProblemAws = do
  replicateM_ 25 (forkChildAws "testScan" testScanAws)
  testScanAws

main :: IO ()
main = do
    logger <- liftIO $ newLogger Debug stdout
    awsEnv <- set envLogger logger <$> newEnv Sydney Discover
    _ <- runResourceT $ runAWST awsEnv replicateProblemAws
    return ()
