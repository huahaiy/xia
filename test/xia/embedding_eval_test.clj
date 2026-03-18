(ns xia.embedding-eval-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [xia.embedding-eval :as eval]
            [xia.test-helpers :as th])
  (:import [java.nio.charset StandardCharsets]
           [java.nio.file Files Path Paths]
           [java.nio.file.attribute FileAttribute]
           [java.util.zip ZipEntry ZipOutputStream]))

(defn- temp-dir
  []
  (str (Files/createTempDirectory "xia-embedding-eval-test"
                                  (into-array FileAttribute []))))

(defn- write-file!
  [path content]
  (let [^Path p (Paths/get path (make-array String 0))]
    (Files/createDirectories (.getParent p) (into-array FileAttribute []))
    (spit path content)))

(defn- write-zip-entry!
  [^ZipOutputStream zip ^String entry-name content]
  (let [^ZipEntry entry (ZipEntry. ^String entry-name)]
    (.putNextEntry zip entry))
  (.write zip ^bytes (.getBytes ^String content StandardCharsets/UTF_8))
  (.closeEntry zip))

(defn- fixture-dataset!
  []
  (let [root (temp-dir)]
    (write-file!
      (str root "/corpus.jsonl")
      (str "{\"_id\":\"d1\",\"title\":\"Garage note\",\"text\":\"The car is parked indoors.\"}\n"
           "{\"_id\":\"d2\",\"title\":\"Kitchen note\",\"text\":\"The soup is simmering.\"}\n"
           "{\"_id\":\"d3\",\"title\":\"Bike note\",\"text\":\"A bicycle is chained outside.\"}\n"))
    (write-file!
      (str root "/queries.jsonl")
      (str "{\"_id\":\"q1\",\"text\":\"automobile\"}\n"
           "{\"_id\":\"q2\",\"text\":\"soup\"}\n"))
    (write-file!
      (str root "/qrels/test.tsv")
      (str "query-id\tcorpus-id\tscore\n"
           "q1\td1\t1\n"
           "q2\td2\t1\n"))
    root))

(defn- fixture-dataset-zip!
  []
  (let [root        (temp-dir)
        archive-path (str root "/tiny-beir.zip")]
    (with-open [zip (ZipOutputStream. (io/output-stream archive-path))]
      (write-zip-entry! zip
                        "tiny-beir/corpus.jsonl"
                        (str "{\"_id\":\"d1\",\"title\":\"Garage note\",\"text\":\"The car is parked indoors.\"}\n"
                             "{\"_id\":\"d2\",\"title\":\"Kitchen note\",\"text\":\"The soup is simmering.\"}\n"))
      (write-zip-entry! zip
                        "tiny-beir/queries.jsonl"
                        (str "{\"_id\":\"q1\",\"text\":\"automobile\"}\n"
                             "{\"_id\":\"q2\",\"text\":\"soup\"}\n"))
      (write-zip-entry! zip
                        "tiny-beir/qrels/test.tsv"
                        (str "query-id\tcorpus-id\tscore\n"
                             "q1\td1\t1\n"
                             "q2\td2\t1\n")))
    archive-path))

(deftest load-beir-dataset-test
  (let [root    (fixture-dataset!)
        dataset (eval/load-dataset {:layout :beir
                                    :path root
                                    :split "test"})]
    (is (= 3 (count (:corpus dataset))))
    (is (= 2 (count (:queries dataset))))
    (is (= {"d1" 1} (get (:qrels dataset) "q1")))))

(deftest evaluate-benchmark-test
  (let [root    (fixture-dataset!)
        report  (eval/evaluate-benchmark
                  {:dataset {:id :tiny-science
                             :label "Tiny science fixture"
                             :layout :beir
                             :path root
                             :split "test"}
                   :models  [{:id :test-synonym-model
                              :label "Test synonym provider"
                              :provider (th/test-embedding-provider)
                              :metric-type :cosine}]
                   :top-ks [1 3]
                   :batch-size 2})]
    (testing "returns one result block"
      (is (= 1 (count (:results report)))))
    (testing "retrieval metrics reflect the semantic hit"
      (let [result (first (:results report))
            metrics (:metrics result)]
        (is (= 1.0 (double (get metrics "recall@1"))))
        (is (= 1.0 (double (get metrics "mrr@1"))))
        (is (= 1.0 (double (get metrics "ndcg@1"))))
        (is (= 32 (:embedding-dimensions result)))))))

(deftest resolve-default-model-adds-provider-dir-test
  (let [resolved (#'eval/resolve-model :datalevin-default
                                       {:cache-dir "/tmp/xia-embedding-eval-test"
                                        :model-path-overrides {}})]
    (is (= :default (get-in resolved [:provider-spec :provider])))
    (is (= "/tmp/xia-embedding-eval-test/providers/datalevin-default"
           (get-in resolved [:provider-spec :dir])))))

(deftest ensure-dataset-and-evaluate-from-archive-test
  (let [archive-path (fixture-dataset-zip!)
        cache-dir    (temp-dir)
        dataset      {:id :tiny-download
                      :label "Tiny download fixture"
                      :layout :beir
                      :split "test"
                      :download {:url (str (.toURI (io/file archive-path)))
                                 :archive-format :zip
                                 :archive-filename "tiny-download.zip"}}
        installed    (eval/ensure-dataset! dataset {:cache-dir cache-dir})]
    (testing "installs into the dataset cache and resolves the nested BEIR root"
      (is (.startsWith ^String (:path installed) cache-dir))
      (is (.endsWith ^String (:path installed) "tiny-beir"))
      (is (.exists (io/file (str (:path installed) "/corpus.jsonl")))))
    (testing "evaluate-benchmark auto-installs when dataset-path is omitted"
      (let [report (eval/evaluate-benchmark
                     {:dataset dataset
                      :cache-dir cache-dir
                      :models [{:id :test-synonym-model
                                :label "Test synonym provider"
                                :provider (th/test-embedding-provider)
                                :metric-type :cosine}]
                      :top-ks [1]})]
        (is (= (:path installed) (get-in report [:dataset :path])))
        (is (= 1.0 (double (get-in report [:results 0 :metrics "recall@1"]))))))))
