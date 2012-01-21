(ns dynamodb.core
  (:import (com.amazonaws.auth BasicAWSCredentials)
           (com.amazonaws.services.dynamodb AmazonDynamoDBClient)
           (com.amazonaws.services.dynamodb.model GetItemRequest PutItemRequest DeleteItemRequest
                                                  ExpectedAttributeValue AttributeValue Key
                                                  KeySchema KeySchemaElement ScalarAttributeType
                                                  CreateTableRequest UpdateTableRequest
                                                  ProvisionedThroughput DeleteTableRequest
                                                  ConditionalCheckFailedException)
           (java.util Map)))

(set! *warn-on-reflection* true)

(defn ^BasicAWSCredentials credentials [^String access-key ^String secret-key]
  (BasicAWSCredentials. access-key secret-key))

(defn dynamodb-client
  "Returns a DynamoDB client instance.

Options include:
:endpoint, provide one of the following endpoint values:

Region                                  Endpoint
US East (Northern Virginia) Region      dynamodb.us-east-1.amazonaws.com
US West (Oregon) Region                 dynamodb.us-west-2.amazonaws.com
US West (Northern California) Region    dynamodb.us-west-1.amazonaws.com
EU (Ireland) Region                     dynamodb.eu-west-1.amazonaws.com
Asia Pacific (Singapore) Region         dynamodb.ap-southeast-1.amazonaws.com
Asia Pacific (Tokyo) Region             dynamodb.ap-northeast-1.amazonaws.com
"
  ([^String access-key ^String secret-key & {:keys [endpoint]}]
     (let [client (AmazonDynamoDBClient. (credentials access-key secret-key))]
       (when endpoint
         (.setEndpoint client endpoint))
       client)))

(defn create-table
  ""
  [^AmazonDynamoDBClient client ^String table-name ^String hash-key-name
   & {:keys [^String range-key-name
             string-hash-key?
             string-range-key?
             ^long kb-read-per-sec
             ^long kb-write-per-sec]
      :or {string-hash-key? true
           string-range-key? true
           kb-read-per-sec 10
           kb-write-per-sec 10}}]
  (let [prov-req (-> (ProvisionedThroughput.)
                     (.withReadCapacityUnits kb-read-per-sec)
                     (.withWriteCapacityUnits kb-write-per-sec))
        hash-key-type (if string-hash-key? ScalarAttributeType/S ScalarAttributeType/N)
        range-key-type (if string-range-key? ScalarAttributeType/S ScalarAttributeType/N)
        hash-key (-> (KeySchemaElement.)
                     (.withAttributeName hash-key-name)
                     (.withAttributeType hash-key-type))
        range-key (when range-key-name
                    (-> (KeySchemaElement.)
                        (.withAttributeName range-key-name)
                        (.withAttributeType range-key-type)))
        key-schema (if range-key
                     (-> (KeySchema. hash-key) (.withRangeKeyElement range-key))
                     (KeySchema. hash-key))
        req (-> (CreateTableRequest. table-name key-schema)
                (.withProvisionedThroughput prov-req))]
    (.createTable client req)))

(defn get-item [^AmazonDynamoDBClient client ^String table-name ^String key-value]
  (let [item (-> (.getItem client (GetItemRequest. table-name (Key. (AttributeValue. key-value))))
                 .getItem)]
    (when item
      (reduce (fn [m k]
                (assoc m k (or (.getS ^AttributeValue (get item k))
                               (.getN ^AttributeValue (get item k)))))
              {} (seq (.keySet item))))))

(defn put-item [^AmazonDynamoDBClient client ^String table-name ^Map item & [^Map expected]]
  (try
    (let [item-map (reduce (fn [m k] (update-in m [k] #(AttributeValue. ^String %))) item (keys item))
          expected-map (when expected
                         (reduce (fn [m k]
                                   (update-in m [k] #(ExpectedAttributeValue. (AttributeValue. ^String %))))
                                 expected (keys expected)))
          req (PutItemRequest. table-name item-map)]
      (if expected
        (.putItem client (.withExpected req expected-map))
        (.putItem client req)))
    (catch ConditionalCheckFailedException e false)))

(defn delete-item [^AmazonDynamoDBClient client ^String table-name ^String key-value]
  (.deleteItem client (DeleteItemRequest. table-name (Key. (AttributeValue. key-value)))))

(defn delete-table [^AmazonDynamoDBClient client ^String table-name]
  (.deleteTable client (DeleteTableRequest. table-name)))

(defn update-provisioned-throughput
  [^AmazonDynamoDBClient client ^String table-name
   ^long kb-read-per-sec ^long kb-write-per-sec]
  (let [prov-req (-> (ProvisionedThroughput.)
                     (.withReadCapacityUnits kb-read-per-sec)
                     (.withWriteCapacityUnits kb-write-per-sec))
        req (-> (UpdateTableRequest.)
                (.withTableName table-name)
                (.withProvisionedThroughput prov-req))]
    (.updateTable client req)))

(comment

  (use 'dynamodb.core :reload-all)
  (def aws-access-key-id (get (System/getenv) "AWS_ACCESS_KEY_ID"))
  (def aws-secret-key (get (System/getenv) "AWS_SECRET_KEY"))
  (def ddb (dynamodb-client aws-access-key-id aws-secret-key))

  (create-table ddb "test" "name")

  (put-item ddb "test" {"name" "a" "value" "0"})

  (get-item ddb "test" "a")

  ;; will work
  (put-item ddb "test" {"name" "a" "value" "1"} {"name" "a", "value" "0"})

  ;; will fail
  (put-item ddb "test" {"name" "a" "value" "2"} {"name" "a", "value" "0"})


  (delete-item ddb "test" "a")

  (delete-table ddb "test")

)
