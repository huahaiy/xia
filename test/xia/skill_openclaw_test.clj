(ns xia.skill-openclaw-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [xia.db :as db]
            [xia.http-client :as http-client]
            [xia.skill.openclaw :as openclaw]
            [xia.test-helpers :as th])
  (:import [java.io ByteArrayOutputStream File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.util.zip ZipEntry ZipOutputStream]))

(use-fixtures :each th/with-test-db)

(defn- temp-dir!
  []
  (.toFile (Files/createTempDirectory "xia-openclaw-test" (into-array FileAttribute []))))

(defn- write-file!
  [^File root relative-path content]
  (let [file (io/file root relative-path)]
    (.mkdirs (.getParentFile file))
    (spit file content)
    file))

(defn- zip-bytes
  [entries]
  (let [out (ByteArrayOutputStream.)]
    (with-open [zip (ZipOutputStream. out)]
      (doseq [[entry-name content] entries]
        (.putNextEntry zip (ZipEntry. ^String entry-name))
        (.write zip (.getBytes ^String content "UTF-8"))
        (.closeEntry zip)))
    (.toByteArray out)))

(deftest import-openclaw-directory-attaches-compatible-content
  (let [^File root (temp-dir!)]
    (try
      (write-file! root
                   "SKILL.md"
                   (str "---\n"
                        "name: Daily Research\n"
                        "description: Check a site and summarize changes.\n"
                        "version: 1.2.3\n"
                        "tags: [browser, research]\n"
                        "homepage: https://example.com/skills/daily-research\n"
                        "---\n\n"
                        "Use the `browser` tool to inspect the target page.\n\n"
                        "See `checklist.md` before you summarize findings.\n"))
      (write-file! root
                   "checklist.md"
                   "# Checklist\n\nCapture title, publish date, and key facts.\n")
      (let [result (openclaw/import-openclaw-source! (.getAbsolutePath root))
            skill  (db/get-skill :daily-research)]
        (is (= :imported-with-warnings (:status result)))
        (is (= "Daily Research" (:skill/name skill)))
        (is (= "1.2.3" (:skill/version skill)))
        (is (= #{:browser :research} (:skill/tags skill)))
        (is (= :openclaw-dir (:skill/source-format skill)))
        (is (= (.getAbsolutePath root) (:skill/source-path skill)))
        (is (true? (:skill/imported-from-openclaw? skill)))
        (is (str/includes? (:skill/content skill) "## Xia Compatibility"))
        (is (str/includes? (:skill/content skill) "OpenClaw `browser`"))
        (is (str/includes? (:skill/content skill) "## Bundled Resources"))
        (is (str/includes? (:skill/content skill) "checklist.md"))
        (is (str/includes? (:skill/content skill) "Capture title, publish date, and key facts."))
        (is (not (str/includes? (:skill/content skill) "homepage:")))
        (is (not (str/includes? (:skill/content skill) "---")))
        (is (some #(str/includes? % "homepage") (:warnings result))))
      (finally
        (doseq [file (reverse (file-seq root))]
          (Files/deleteIfExists (.toPath ^File file)))))))

(deftest import-openclaw-url-zip
  (let [archive (zip-bytes
                  {"daily-research/SKILL.md"
                   (str "---\n"
                        "name: Remote Research\n"
                        "description: Imported from zip.\n"
                        "tags: [search]\n"
                        "---\n\n"
                        "Use the `search` tool first.\n")})]
    (with-redefs [http-client/request (fn [_]
                                        {:status 200
                                         :headers {"content-type" "application/zip"}
                                         :body archive})]
      (let [result (openclaw/import-openclaw-source! "https://clawhub.ai/downloads/remote-research.zip")
            skill  (db/get-skill :remote-research)]
        (is (= :openclaw-zip-url (:skill/source-format skill)))
        (is (= "https://clawhub.ai/downloads/remote-research.zip"
               (:skill/source-url skill)))
        (is (= :remote-research (:skill-id result)))
        (is (str/includes? (:skill/content skill) "Xia `web-search`"))))))

(deftest import-openclaw-rejects-blocked-runtime-metadata
  (let [^File root (temp-dir!)]
    (try
      (write-file! root
                   "SKILL.md"
                   (str "---\n"
                        "name: Blocked Skill\n"
                        "metadata: {\"openclaw\":{\"requires\":[\"browser\"]}}\n"
                        "---\n\n"
                        "Do a thing.\n"))
      (try
        (openclaw/import-openclaw-source! (.getAbsolutePath root))
        (is false "Expected blocked OpenClaw metadata to reject the import")
        (catch clojure.lang.ExceptionInfo e
          (is (= :rejected (:status (ex-data e))))
          (is (some #(str/includes? % "requires") (:errors (ex-data e))))
          (is (nil? (db/get-skill :blocked-skill)))))
      (finally
        (doseq [file (reverse (file-seq root))]
          (Files/deleteIfExists (.toPath ^File file)))))))
