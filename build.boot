(set-env!
  :resource-paths #{"src" "resources"}
  :source-paths #{"test"}
  :dependencies '[[adzerk/boot-test "1.1.2" :scope "test"]
                  [adzerk/bootlaces "0.1.13" :scope "test"]
                  [com.amazonaws/dynamodb-streams-kinesis-adapter "1.1.1"]
                  [com.stuartsierra/component "0.3.2"]
                  [org.clojure/clojure "1.8.0" :scope "provided"]
                  [org.clojure/core.async "0.2.395"]
                  [org.clojure/tools.logging "0.3.1"]])

(require '[adzerk.boot-test :refer [test]]
         '[adzerk.bootlaces :refer [bootlaces! build-jar push-snapshot]])

(def +version+ "0.1.0-SNAPSHOT")
(bootlaces! +version+)

(task-options!
  pom {:project 'rads/dynamodb-streams-consumer
       :version +version+
       :description "DynamoDB Streams Consumer"
       :url "https://github.com/rads/dynamodb-streams-consumer"
       :scm {:url "https://github.com/rads/dynamodb-streams-consumer"}
       :license {"MIT License"
                 "http://www.opensource.org/licenses/mit-license.php"}})

(deftask install-locally
  []
  (comp (pom) (jar) (install)))

(deftask publish
  []
  (comp (pom) (jar) (push)))
