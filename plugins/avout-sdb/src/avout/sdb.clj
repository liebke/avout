(ns avout.sdb
  (:import (com.amazonaws.auth BasicAWSCredentials)
           (com.amazonaws.services.simpledb AmazonSimpleDBClient)
           (com.amazonaws.services.simpledb.model GetAttributesRequest PutAttributesRequest
                                                  ReplaceableAttribute Attribute UpdateCondition
                                                  CreateDomainRequest DeleteAttributesRequest)))

(defn credentials [access-key secret-key]
  (BasicAWSCredentials. access-key secret-key))

(defn sdb-client [access-key secret-key]
  (AmazonSimpleDBClient. (credentials access-key secret-key)))

(defn create-domain [client domain-name]
  (.createDomain client (CreateDomainRequest. domain-name)))

(defn get-attributes [client domain-name item-name]
  (-> (.getAttributes client (GetAttributesRequest. domain-name item-name))
      .getAttributes
      (->> (reduce #(assoc %1 (.getName %2) (.getValue %2)) {}))))

(defn put-attributes [client domain-name item-name attributes & [condition]]
  (let [attrs (for [{:keys [name value replace] :or {replace true}} attributes]
                (ReplaceableAttribute. name value replace))]
    (try
      (.putAttributes client
                      (if-let [{:keys [name value exists]} condition]
                        (PutAttributesRequest. domain-name item-name attrs
                                               (UpdateCondition. name value exists))
                        (PutAttributesRequest. domain-name item-name attrs)))
      true
      (catch com.amazonaws.AmazonServiceException e
        (if (.startsWith (.getMessage e) "Conditional check failed")
          false
          (throw e))))))

(defn delete-attributes [client domain-name item-name attrs]
  (.deleteAttributes client (DeleteAttributesRequest. domain-name item-name
                                                      (for [{:keys [name]} attrs]
                                                        (-> (Attribute.) (.setName name))))))

(comment

  (use 'avout.sdb :reload-all)
  (def ACCESS-KEY (get (System/getenv) "ACCESS_KEY"))
  (def SECRET-KEY (get (System/getenv) "SECRET_KEY"))
  (def sdb (sdb-client ACCESS-KEY SECRET-KEY))
  (create-domain sdb "test-domain")

  (put-attributes sdb "test-domain" "a" [{:name "value" :value "0" :replace true}])

  (get-attributes sdb "test-domain" "a")

  (put-attributes sdb "test-domain" "a"
                  [{:name "value" :value "1" :replace true}]
                  {:name "value" :value "0"})

  (get-attributes sdb "test-domain" "a")

  (put-attributes sdb "test-domain" "a"
                  [{:name "value" :value "2" :replace true}]
                  {:name "value" :value "0"})

  (get-attributes sdb "test-domain" "a")

  (delete-attributes sdb "test-domain" "a" [{:name "value"}])

)