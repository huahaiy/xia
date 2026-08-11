(ns xia.pack-security-test
  (:require [clojure.test :refer [deftest is]]
            [xia.local-doc]
            [xia.pack :as pack])
  (:import [java.io BufferedOutputStream ByteArrayOutputStream FileOutputStream]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.util.zip ZipEntry ZipOutputStream]))

(defn- temp-paths
  []
  (let [dir (Files/createTempDirectory
             "xia-pack-security-test"
             (make-array FileAttribute 0))]
    {:archive (str (.resolve dir "input.xia"))
     :dest (str (.resolve dir "expanded"))}))

(defn- write-zip!
  [archive entries]
  (with-open [fos (FileOutputStream. ^String archive)
              zip (-> fos BufferedOutputStream. ZipOutputStream.)]
    (doseq [[entry-name data] entries]
      (.putNextEntry zip (ZipEntry. ^String entry-name))
      (.write zip (.getBytes ^String data StandardCharsets/UTF_8))
      (.closeEntry zip))))

(defn- unpack-error
  [archive dest & opts]
  (try
    (apply pack/unpack! archive dest opts)
    nil
    (catch clojure.lang.ExceptionInfo e
      e)))

(defn- zip-bytes
  [entries]
  (let [bytes (ByteArrayOutputStream.)]
    (with-open [zip (ZipOutputStream. bytes)]
      (doseq [[entry-name data] entries]
        (.putNextEntry zip (ZipEntry. ^String entry-name))
        (.write zip (.getBytes ^String data StandardCharsets/UTF_8))
        (.closeEntry zip)))
    (.toByteArray bytes)))

(deftest archive-expansion-limit-stops-before-disk-exhaustion
  (let [{:keys [archive dest]} (temp-paths)]
    (write-zip! archive [["db/data.mdb" (apply str (repeat 4096 "x"))]
                         ["manifest.edn" "{:format :xia-pack/v1}"]])
    (let [ex (unpack-error archive dest
                           :max-entry-bytes 8192
                           :max-expanded-bytes 1024)]
      (is (= :archive/limit-exceeded (:type (ex-data ex))))
      (is (= :expanded-bytes (:limit (ex-data ex))))
      (is (= "db/data.mdb" (:entry (ex-data ex))))
      (is (not (Files/exists (java.nio.file.Paths/get dest (make-array String 0))
                             (make-array java.nio.file.LinkOption 0)))
          "a failed extraction must not leave a partially expanded archive"))))

(deftest archive-entry-count-is-bounded
  (let [{:keys [archive dest]} (temp-paths)]
    (write-zip! archive [["db/a" "a"]
                         ["db/b" "b"]
                         ["manifest.edn" "{:format :xia-pack/v1}"]])
    (let [ex (unpack-error archive dest :max-entries 2)]
      (is (= :archive/limit-exceeded (:type (ex-data ex))))
      (is (= :entry-count (:limit (ex-data ex))))
      (is (= 3 (:actual (ex-data ex)))))))

(deftest office-document-archive-expansion-is-bounded
  (let [extract-archive (var-get (ns-resolve 'xia.local-doc 'zip-entry-bytes-map))
        max-entry-var (ns-resolve 'xia.local-doc 'office-archive-max-entry-bytes)
        max-total-var (ns-resolve 'xia.local-doc 'office-archive-max-expanded-bytes)
        archive (zip-bytes [["word/document.xml" (apply str (repeat 128 "x"))]])
        ex (with-redefs-fn {max-entry-var 32
                            max-total-var 64}
             #(try
                (extract-archive archive)
                nil
                (catch clojure.lang.ExceptionInfo e
                  e)))]
    (is (= :local-doc/archive-limit-exceeded (:type (ex-data ex))))
    (is (= :entry-bytes (:limit (ex-data ex))))
    (is (= "word/document.xml" (:entry (ex-data ex))))))
