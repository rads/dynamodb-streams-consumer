(ns rads.dynamodb-streams-converter
  (:require
    [clojure.core.async :as async :refer [go <!]]
    [clojure.tools.logging :as log]
    [com.stuartsierra.component :as component])
  (:import
    (com.amazonaws ClientConfiguration)
    (com.amazonaws.auth EnvironmentVariableCredentialsProvider)
    (com.amazonaws.services.kinesis.clientlibrary.interfaces
      IRecordProcessor
      IRecordProcessorFactory)
    (com.amazonaws.services.cloudwatch AmazonCloudWatchClient)
    (com.amazonaws.services.dynamodbv2 AmazonDynamoDBClient)
    (com.amazonaws.services.dynamodbv2.streamsadapter
      AmazonDynamoDBStreamsAdapterClient)
    (com.amazonaws.services.kinesis.clientlibrary.lib.worker
      InitialPositionInStream
      KinesisClientLibConfiguration
      Worker)
    (com.amazonaws.services.kinesis.clientlibrary.types
      ShutdownReason)))

(defn new-records-event [records]
  {:id (java.util.UUID/randomUUID)
   :action :new-records
   :data records})

(defrecord DDBRecordProcessor [pub out]
  IRecordProcessor
  (initialize [_ shard-id]
    (log/info "Initializing record processor with shard-id" shard-id "..."))
  (processRecords [_ records checkpointer]
    (let [event (new-records-event records)
          ack (async/promise-chan (filter #(= (:data %) (:id event))))]
      (async/sub pub :ack ack)
      (async/put! out event)
      (go
        (<! ack)
        (log/info "Acked records...")
        (async/unsub pub :ack ack)
        (.checkpoint checkpointer))))
  (shutdown [_ checkpointer reason]
    (log/info "Shutting down record processor" reason)
    (when (= reason ShutdownReason/TERMINATE)
      (.checkpoint checkpointer))))

(defrecord DDBRecordProcessorFactory [pub out]
  IRecordProcessorFactory
  (createProcessor [_]
    (map->DDBRecordProcessor {:pub pub :out out})))

(defn new-record-processor-factory []
  (map->DDBRecordProcessorFactory {}))

(defrecord DDBClient [ddb-credentials ddb-endpoint]
  component/Lifecycle
  (start [this]
    (let [client (doto (AmazonDynamoDBClient. ddb-credentials
                                              (ClientConfiguration.))
                       (.setEndpoint ddb-endpoint))]
      (assoc this :client client)))
  (stop [this]
    (assoc this :client nil)))

(defn new-ddb-client []
  (map->DDBClient {}))

(defrecord CloudWatchClient [ddb-credentials]
  component/Lifecycle
  (start [this]
    (let [client (AmazonCloudWatchClient. ddb-credentials
                                          (ClientConfiguration.))]
      (assoc this :client client)))
  (stop [this]
    (assoc this :client nil)))

(defn new-cloudwatch-client []
  (map->CloudWatchClient {}))

(defrecord DDBAdapterClient [streams-credentials streams-endpoint]
  component/Lifecycle
  (start [this]
    (let [client (doto (AmazonDynamoDBStreamsAdapterClient. streams-credentials
                                                            (ClientConfiguration.))
                       (.setEndpoint streams-endpoint))]
      (assoc this :client client)))
  (stop [this]
    (assoc this :client nil)))

(defn new-adapter-client []
  (map->DDBAdapterClient {}))

(defrecord DDBWorkerConfig [app-name worker-id stream-arn streams-credentials max-records idle-time]
  component/Lifecycle
  (start [this]
    (let [trim-horizon InitialPositionInStream/TRIM_HORIZON
          config (doto (KinesisClientLibConfiguration. app-name
                                                       stream-arn
                                                       streams-credentials
                                                       worker-id)
                       (.withMaxRecords max-records)
                       (.withIdleTimeBetweenReadsInMillis idle-time)
                       (.withInitialPositionInStream trim-horizon))]
      (assoc this :config config)))
  (stop [this]
    (assoc this :config nil)))

(defn new-worker-config [params]
  (map->DDBWorkerConfig (select-keys params [:app-name :worker-id :stream-arn
                                             :max-records :idle-time])))

(defrecord DDBWorker [record-processor-factory worker-config adapter-client
                      ddb-client cloudwatch-client worker thread]
  component/Lifecycle
  (start [this]
    (log/info "Starting worker...")
    (let [worker (Worker. record-processor-factory
                          (:config worker-config)
                          (:client adapter-client)
                          (:client ddb-client)
                          (:client cloudwatch-client))
          thread (Thread. worker)]
      (.start thread)
      (merge this {:worker worker, :thread thread})))
  (stop [this]
    (log/info "Stopping worker...")
    (.stop thread)
    (merge this {:worker nil
                 :thread nil})))

(defn new-worker []
  (map->DDBWorker {}))

(defn new-credentials []
  (EnvironmentVariableCredentialsProvider.))

(defn new-subsystem [config]
  (let [out (async/chan)
        in (async/chan)
        pub (async/pub in :action)]
    (-> (component/system-map
          :adapter-client (new-adapter-client)
          :cloudwatch-client (new-cloudwatch-client)
          :ddb-client (new-ddb-client)
          :ddb-credentials (new-credentials)
          :ddb-endpoint (:ddb-endpoint config)
          :in in
          :out out
          :pub pub
          :record-processor-factory (new-record-processor-factory)
          :streams-credentials (new-credentials)
          :streams-endpoint (:streams-endpoint config)
          :worker (new-worker)
          :worker-config (new-worker-config (:worker-config config)))
        (component/system-using
          {:adapter-client [:streams-credentials :streams-endpoint]
           :cloudwatch-client [:ddb-credentials]
           :ddb-client [:ddb-credentials :ddb-endpoint]
           :record-processor-factory [:pub :out]
           :worker-config [:streams-credentials]
           :worker [:record-processor-factory :worker-config :adapter-client
                    :ddb-client :cloudwatch-client]}))))

(defrecord Consumer [config subsystem in out]
  component/Lifecycle
  (start [this]
    (log/info "Starting consumer...")
    (let [subsystem (component/start (new-subsystem config))]
      (merge this {:subsystem subsystem
                   :in (:in subsystem)
                   :out (:out subsystem)})))
  (stop [this]
    (log/info "Stopping consumer...")
    (component/stop subsystem)
    (merge this {:subsystem nil
                 :in nil
                 :out nil})))

(defn new-consumer [config]
  (map->Consumer {:config config}))
