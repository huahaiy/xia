(ns build
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.tools.namespace.parse :as ns-parse]
            [clojure.tools.build.api :as b])
  (:import [java.net URI]
           [java.nio.file FileSystems Files LinkOption]
           [java.util.zip ZipFile]))

(def lib 'com.xia/xia)
(def version "0.1.0")
(def class-dir "target/classes")
(def uber-file "target/xia.jar")
(def test-class-dir "target/test-classes")
(def test-uber-file "target/xia-tests.jar")
(def generated-test-java-dir "target/generated-test-java")
(def basis (delay (b/create-basis {:project "deps.edn"})))
(def test-basis (delay (b/create-basis {:project "deps.edn"
                                        :aliases [:test]})))
(def compiler-bindings
  {#'*warn-on-reflection* true
   #'*unchecked-math* :warn-on-boxed})

(def ^:private native-resource-config
  "META-INF/native-image/xia/xia/resource-config.json")
(def ^:private native-test-reflect-config
  "META-INF/native-image/xia/xia-tests/reflect-config.json")
(def ^:private native-test-native-image-properties
  "META-INF/native-image/xia/xia-tests/native-image.properties")
(def ^:private datalevin-native-image-properties
  "META-INF/native-image/datalevin/datalevin/native-image.properties")
(declare ns-sym->class-name)

(def ^:private common-resource-patterns
 ["oauth_providers.edn"
   "llm_provider_templates.edn"
   "example-tool.edn"
   "example-skill.md"
   "eval/"
   "log4j2.component.properties"
   "log4j2.simplelog.properties"
   "org/openpdf/"
   "tools/"
   "web/"])

(defn- resource-pattern
  [path]
  (str "\\\\Q" path "\\\\E.*"))

(defn- native-driver-resource-dir
  []
  (let [os-name (some-> (System/getProperty "os.name") str/lower-case)
        arch    (some-> (System/getProperty "os.arch") str/lower-case)]
    (cond
      (and (str/includes? os-name "mac")
           (#{"aarch64" "arm64"} arch))
      "driver/mac-arm64/"

      (and (str/includes? os-name "linux")
           (#{"aarch64" "arm64"} arch))
      "driver/linux-arm64/"

      (and (str/includes? os-name "linux")
           (#{"x86_64" "amd64"} arch))
      "driver/linux/"

      (and (str/includes? os-name "windows")
           (#{"x86_64" "amd64"} arch))
      "driver/win32_x64/"

      :else
      (throw (ex-info "Unsupported platform for Playwright native driver packaging."
                      {:os-name os-name
                       :arch arch})))))

(defn- write-native-resource-config!
  ([target-class-dir]
   (write-native-resource-config! target-class-dir []))
  ([target-class-dir extra-patterns]
  (let [path     (str target-class-dir "/" native-resource-config)
        patterns (map resource-pattern
                      (concat common-resource-patterns
                              extra-patterns
                              [(native-driver-resource-dir)]))
        body     (str "{\n"
                      "  \"resources\": {\n"
                      "    \"includes\": [\n"
                      (str/join ",\n"
                                (map (fn [pattern]
                                       (str "      {\n"
                                            "        \"pattern\": \"" pattern "\"\n"
                                            "      }"))
                                     patterns))
                      "\n"
                      "    ]\n"
                      "  }\n"
                      "}\n")]
    (io/make-parents path)
    (spit path body))))

(defn- test-namespace-symbols
  []
  (->> (file-seq (io/file "test"))
       (filter #(.isFile ^java.io.File %))
       (map #(.getPath ^java.io.File %))
       (filter #(str/ends-with? % "_test.clj"))
       (map #(-> %
                 (str/replace #"^test[\\/]" "")
                 (str/replace #"\.clj$" "")
                 (str/replace #"_" "-")
                 (str/replace #"[\\/]" ".")))
       sort
       (mapv symbol)))

(def ^:private native-test-dev-only-namespaces
  '#{xia.dev-repl-test
     xia.dev-nrepl-test
     xia.embedding-eval-test
     xia.summarization-eval-test})

(def ^:private native-test-dependency-namespaces
  '[datalevin.embedding
    datalevin.llm])

(def ^:private native-test-jvm-only-namespaces
  native-test-dev-only-namespaces)

(def ^:private native-test-source-dependency-prefixes
  ["borkdude/graal/"
   "charred/"
   "clj_commons/"
   "cognitect/"
   "crypto/"
   "edamame/"
   "hato/"
   "integrant/"
   "jsonista/"
   "nextjournal/"
   "nrepl/"
   "org/httpkit/"
   "ring/"
   "sci/"
   "taoensso/"
   "weavejester/"])

(def ^:private native-test-discovery-begin-marker
  "__XIA_NATIVE_TEST_NAMESPACES_BEGIN__")

(def ^:private native-test-discovery-end-marker
  "__XIA_NATIVE_TEST_NAMESPACES_END__")

(defn- actual-test-namespace?
  [ns-sym]
  (str/ends-with? (name ns-sym) "-test"))

(defn- test-support-namespace-symbols
  []
  (->> (test-namespace-symbols)
       (remove actual-test-namespace?)
       vec))

(defn- native-test-namespace-symbols
  []
  (->> (test-namespace-symbols)
       (filter actual-test-namespace?)
       (remove native-test-jvm-only-namespaces)
       vec))

(defn- parse-native-test-targets
  [opts]
  (let [available     (native-test-namespace-symbols)
        available-set (set available)
        raw           (or (:namespaces opts)
                          (System/getenv "XIA_NATIVE_TEST_NAMESPACES"))]
    (if (or (nil? raw)
            (and (string? raw) (str/blank? raw)))
      available
      (let [selected (->> (cond
                            (string? raw)
                            (->> (str/split raw #",")
                                 (map str/trim)
                                 (remove str/blank?))

                            (sequential? raw)
                            raw

                            :else
                            [raw])
                          (map (fn [entry]
                                 (cond
                                   (symbol? entry) entry
                                   (keyword? entry) (symbol (name entry))
                                   (string? entry) (symbol entry)
                                   :else (throw (ex-info "Unsupported native test namespace selector."
                                                         {:value entry})))))
                          vec)
            unknown  (remove available-set selected)]
        (when (empty? selected)
          (throw (ex-info "No native test namespaces were selected."
                          {:available available})))
        (when (seq unknown)
          (throw (ex-info "Unknown or JVM-only native test namespaces."
                          {:unknown unknown
                           :available available
                           :jvm-only native-test-jvm-only-namespaces})))
        selected))))

(defn- native-test-discovery-form
  [selected-test-nss]
  (str "(require 'clojure.java.io 'clojure.string)\n"
       "(let [before# (set (loaded-libs))\n"
       "      targets# '" (pr-str selected-test-nss) "]\n"
       "  (doseq [ns# targets#]\n"
       "    (require ns#))\n"
       "  (let [loaded# (->> (concat targets# (remove before# (loaded-libs)))\n"
       "                     distinct\n"
       "                     (remove #{'user})\n"
       "                     (filter (fn [ns#]\n"
       "                               (let [path# (-> ns# name (clojure.string/replace \"-\" \"_\") (clojure.string/replace \".\" \"/\"))]\n"
       "                                 (or (clojure.java.io/resource (str path# \".clj\"))\n"
       "                                     (clojure.java.io/resource (str path# \".cljc\"))))))\n"
       "                     sort\n"
       "                     vec)]\n"
       "    (println \"" native-test-discovery-begin-marker "\")\n"
       "    (prn loaded#)\n"
       "    (println \"" native-test-discovery-end-marker "\")))"))

(defn- discover-native-test-compile-namespaces
  [selected-test-nss]
  (let [java-bin (str (System/getProperty "java.home")
                      java.io.File/separator
                      "bin"
                      java.io.File/separator
                      "java")
        cp       (str/join java.io.File/pathSeparator
                           (:classpath-roots @test-basis))
        {:keys [exit out err]}
        (sh/sh java-bin "-cp" cp "clojure.main" "-e"
               (native-test-discovery-form selected-test-nss))]
    (when-not (zero? exit)
      (throw (ex-info "Failed to discover native test namespace closure."
                      {:selected selected-test-nss
                       :exit exit
                       :out out
                       :err err})))
    (let [[_ body] (re-find (re-pattern (str "(?s)"
                                            native-test-discovery-begin-marker
                                            "\\s*(\\[.*?\\])\\s*"
                                            native-test-discovery-end-marker))
                            out)]
      (when-not body
        (throw (ex-info "Failed to parse native test namespace discovery output."
                        {:selected selected-test-nss
                         :out out
                         :err err})))
      (edn/read-string body))))

(defn- clj-resource-path?
  [path]
  (or (str/ends-with? path ".clj")
      (str/ends-with? path ".cljc")))

(defn- ns-sym->resource-stems
  [ns-sym]
  (-> (name ns-sym)
      (str/replace "-" "_")
      (str/replace "." "/")))

(defn- resource-path->ns-sym
  [path]
  (-> path
      (str/replace #"\.(clj|cljc)$" "")
      (str/replace #"_" "-")
      (str/replace #"[\\/]" ".")
      symbol))

(defn- scan-classpath-clj-resources
  [root]
  (let [root-file (io/file root)]
    (cond
      (.isDirectory root-file)
      (->> (file-seq root-file)
           (filter #(.isFile ^java.io.File %))
           (map #(.getPath ^java.io.File %))
           (map #(subs % (inc (count (.getPath root-file)))))
           (filter clj-resource-path?))

      (and (.isFile root-file)
           (str/ends-with? (.getName root-file) ".jar"))
      (with-open [zip (ZipFile. root-file)]
        (->> (enumeration-seq (.entries zip))
             (map #(.getName ^java.util.zip.ZipEntry %))
             (filter clj-resource-path?)
             vec))

      :else
      [])))

(defn- native-test-source-dependency-namespaces
  []
  (->> (:classpath-roots @test-basis)
       (mapcat scan-classpath-clj-resources)
       (filter (fn [path]
                 (some #(str/starts-with? path %)
                       native-test-source-dependency-prefixes)))
       (map resource-path->ns-sym)
       distinct
       sort
       vec))

(defn- read-classpath-resource
  [resource-path]
  (some (fn [root]
          (let [root-file (io/file root)]
            (cond
              (.isDirectory root-file)
              (let [candidate (io/file root-file resource-path)]
                (when (.isFile candidate)
                  (slurp candidate)))

              (and (.isFile root-file)
                   (str/ends-with? (.getName root-file) ".jar"))
              (with-open [zip (ZipFile. root-file)]
                (when-let [entry (.getEntry zip resource-path)]
                  (slurp (.getInputStream zip entry))))

              :else
              nil)))
        (:classpath-roots @test-basis)))

(defn- ns-source-body
  [ns-sym]
  (let [stem (ns-sym->resource-stems ns-sym)]
    (or (read-classpath-resource (str stem ".clj"))
        (read-classpath-resource (str stem ".cljc")))))

(defn- namespace-deps
  [ns-sym]
  (when-let [body (ns-source-body ns-sym)]
    (with-open [reader (java.io.PushbackReader. (java.io.StringReader. body))]
      (some-> reader
              ns-parse/read-ns-decl
              ns-parse/deps-from-ns-decl))))

(defn- static-native-test-compile-namespaces
  [selected-test-nss]
  (loop [pending (vec selected-test-nss)
         seen    #{}]
    (if-let [ns-sym (first pending)]
      (if (seen ns-sym)
        (recur (subvec pending 1) seen)
        (let [deps (or (namespace-deps ns-sym) #{})]
          (recur (into (subvec pending 1) deps)
                 (conj seen ns-sym))))
      (->> seen
           sort
           vec))))

(defn- copy-native-test-sources!
  [selected-test-nss]
  (b/copy-dir {:src-dirs ["src" "resources" "test"]
               :target-dir test-class-dir})
  (when (some native-test-dev-only-namespaces selected-test-nss)
    (b/copy-dir {:src-dirs ["dev/xia"]
                 :target-dir (str test-class-dir "/xia")})
    (b/copy-dir {:src-dirs ["dev-resources"]
                 :target-dir test-class-dir})))

(defn- ns-sym->class-name
  [ns-sym]
  (-> (name ns-sym)
      (str/replace "-" "_")))

(defn- init-class-name->ns-sym
  [class-name]
  (-> class-name
      (str/replace #"__init$" "")
      (str/replace "_" "-")
      symbol))

(defn- write-native-test-main-java!
  [selected-test-nss support-nss reachable-init-class-names]
  (let [path        (str generated-test-java-dir "/xia/NativeTestMain.java")
        init-classes reachable-init-class-names]
    (io/make-parents path)
    (spit path
          (str "package xia;\n\n"
               "import clojure.lang.IFn;\n"
               "import clojure.lang.Keyword;\n"
               "import clojure.lang.RT;\n\n"
               "public final class NativeTestMain {\n"
               "  private static final String[] ALL_TEST_NAMESPACES = new String[] {"
               (->> selected-test-nss
                    (map #(str "\"" % "\""))
                    (str/join ", "))
               "};\n\n"
               "  private static final Class<?>[] REACHABLE_INIT_CLASSES = new Class<?>[] {\n"
               (->> init-classes
                    (map #(str "    " % ".class"))
                    (str/join ",\n"))
               "\n  };\n\n"
               "  private NativeTestMain() {}\n\n"
               "  private static void ensureReachableInitClasses() {\n"
               "    if (REACHABLE_INIT_CLASSES.length == 0 && ALL_TEST_NAMESPACES.length == 0) {\n"
               "      throw new IllegalStateException(\"No test namespaces compiled into native test image.\");\n"
               "    }\n"
               "  }\n\n"
               "  private static boolean namespaceLoaded(String nsName) {\n"
               "    return clojure.lang.Namespace.find(clojure.lang.Symbol.intern(nsName)) != null;\n"
               "  }\n\n"
               "  private static boolean testsAlreadyLoaded() {\n"
               "    clojure.lang.Namespace ns = clojure.lang.Namespace.find(clojure.lang.Symbol.intern(\"clojure.test\"));\n"
               "    if (ns == null) {\n"
               "      return false;\n"
               "    }\n"
               "    clojure.lang.Var runTests = ns.findInternedVar(clojure.lang.Symbol.intern(\"run-tests\"));\n"
               "    return runTests != null && runTests.getRawRoot() instanceof IFn;\n"
               "  }\n\n"
               "  private static void withNamespaceLoadContext(Runnable action) {\n"
               "    Object currentNs = clojure.lang.Namespace.findOrCreate(clojure.lang.Symbol.intern(\"user\"));\n"
               "    clojure.lang.Var.pushThreadBindings(RT.map(RT.CURRENT_NS, currentNs));\n"
               "    try {\n"
               "      action.run();\n"
               "    } finally {\n"
               "      clojure.lang.Var.popThreadBindings();\n"
               "    }\n"
               "  }\n\n"
               "  private static void loadSupportNamespaces() {\n"
               (apply str
                      (map (fn [ns-sym]
                             (str "    " (ns-sym->class-name ns-sym) "__init.load();\n"))
                           support-nss))
               "  }\n\n"
               "  private static void loadTestNamespace(String ns) {\n"
               "    switch (ns) {\n"
               (apply str
                      (map (fn [ns-sym]
                             (str "      case \"" ns-sym "\":\n"
                                  "        " (ns-sym->class-name ns-sym) "__init.load();\n"
                                  "        return;\n"))
                           selected-test-nss))
               "      default:\n"
               "        throw new IllegalArgumentException(\"Unknown test namespace: \" + ns);\n"
               "    }\n"
               "  }\n\n"
               "  private static String[] selectTestNamespaces(String[] args) {\n"
               "    if (args.length == 0) {\n"
               "      return ALL_TEST_NAMESPACES;\n"
               "    }\n"
               "    for (String arg : args) {\n"
               "      boolean found = false;\n"
               "      for (String ns : ALL_TEST_NAMESPACES) {\n"
               "        if (ns.equals(arg)) {\n"
               "          found = true;\n"
               "          break;\n"
               "        }\n"
               "      }\n"
               "      if (!found) {\n"
               "        System.err.println(\"Unknown test namespaces: \" + arg);\n"
               "        System.err.print(\"Available namespaces: \");\n"
               "        for (int i = 0; i < ALL_TEST_NAMESPACES.length; i++) {\n"
               "          if (i > 0) System.err.print(\", \");\n"
               "          System.err.print(ALL_TEST_NAMESPACES[i]);\n"
               "        }\n"
               "        System.err.println();\n"
               "        System.exit(2);\n"
               "      }\n"
               "    }\n"
               "    return args;\n"
               "  }\n\n"
               "  public static void main(String[] args) {\n"
               "    String[] selected = selectTestNamespaces(args);\n"
               "    ensureReachableInitClasses();\n"
               "    withNamespaceLoadContext(() -> {\n"
               "      if (!testsAlreadyLoaded()) {\n"
               "        clojure.test__init.load();\n"
               "      }\n"
               (apply str
                      (map (fn [ns-sym]
                             (str "      if (!namespaceLoaded(\"" ns-sym "\")) {\n"
                                  "        " (ns-sym->class-name ns-sym) "__init.load();\n"
                                  "      }\n"))
                           support-nss))
               "      for (String ns : selected) {\n"
               "        if (!namespaceLoaded(ns)) {\n"
               "          loadTestNamespace(ns);\n"
               "        }\n"
               "      }\n"
               "    });\n"
               "    Object[] symbols = new Object[selected.length];\n"
               "    for (int i = 0; i < selected.length; i++) {\n"
               "      symbols[i] = clojure.lang.Symbol.intern(selected[i]);\n"
               "    }\n"
               "    IFn runTests = RT.var(\"clojure.test\", \"run-tests\");\n"
               "    Object summary = runTests.applyTo(RT.seq(symbols));\n"
               "    RT.var(\"clojure.core\", \"shutdown-agents\").invoke();\n"
               "    Object fail = RT.get(summary, Keyword.intern(null, \"fail\"));\n"
               "    Object error = RT.get(summary, Keyword.intern(null, \"error\"));\n"
               "    int failures = (fail instanceof Number ? ((Number) fail).intValue() : 0)\n"
               "                 + (error instanceof Number ? ((Number) error).intValue() : 0);\n"
               "    System.exit(failures == 0 ? 0 : 1);\n"
               "  }\n"
               "}\n"))))

(defn- scan-classpath-init-class-names
  [root]
  (let [root-file (io/file root)]
    (cond
      (.isDirectory root-file)
      (->> (file-seq root-file)
           (filter #(.isFile ^java.io.File %))
           (map #(.getPath ^java.io.File %))
           (filter #(str/ends-with? % "__init.class"))
           (map #(subs % (inc (count (.getPath root-file)))))
           (map #(-> %
                     (str/replace #"\.class$" "")
                     (str/replace #"[\\/]" ".")))
           vec)

      (and (.isFile root-file)
           (str/ends-with? (.getName root-file) ".jar"))
      (with-open [zip (ZipFile. root-file)]
        (->> (enumeration-seq (.entries zip))
             (map #(.getName ^java.util.zip.ZipEntry %))
             (filter #(str/ends-with? % "__init.class"))
             (map #(-> %
                       (str/replace #"\.class$" "")
                       (str/replace #"[\\/]" ".")))
             vec))

      :else
      [])))

(defn- archive-class-names-for-namespaces
  [archive-path ns-syms]
  (let [stems (map ns-sym->resource-stems ns-syms)]
    (with-open [zip (ZipFile. archive-path)]
      (->> (enumeration-seq (.entries zip))
           (map #(.getName ^java.util.zip.ZipEntry %))
           (keep (fn [entry]
                   (some (fn [stem]
                           (let [prefix (str stem "/")]
                             (when (str/starts-with? entry prefix)
                               (let [remainder (subs entry (count prefix))]
                                 (when (and (str/ends-with? remainder ".class")
                                            (not (str/includes? remainder "/"))
                                            (not (str/ends-with? remainder "__init.class"))
                                            (not (str/includes? remainder "$")))
                                   (-> entry
                                       (str/replace #"\.class$" "")
                                       (str/replace #"[\\/]" ".")))))))
                         stems)))
           distinct
           sort
           vec))))

(defn- native-test-reachable-init-class-names
  []
  (->> (concat (:classpath-roots @test-basis)
               [test-class-dir])
       (mapcat scan-classpath-init-class-names)
       (remove #(str/starts-with? % "datalevin."))
       (remove #(str/starts-with? % "clj_easy.graal_build_time."))
       distinct
       sort
       vec))

(defn- write-archive-text-entry!
  [archive-path entry-path body]
  (let [archive-uri (URI/create (str "jar:" (.toURI (io/file archive-path))))
        link-opts   (into-array LinkOption [])
        path-parts  (into-array String [])]
    (with-open [fs (FileSystems/newFileSystem archive-uri {"create" "false"})]
      (let [entry (.getPath fs (str "/" entry-path) path-parts)]
        (when-let [parent (.getParent entry)]
          (when-not (Files/exists parent link-opts)
            (Files/createDirectories parent
                                     (into-array java.nio.file.attribute.FileAttribute []))))
        (Files/write entry
                     (.getBytes body "UTF-8")
                     (into-array java.nio.file.OpenOption []))))))

(defn- write-native-test-reflect-config!
  [archive-path class-names]
  (let [body (str "[\n"
                  (str/join ",\n"
                            (map (fn [class-name]
                                   (str "  {\n"
                                        "    \"name\": \"" class-name "\"\n"
                                        "  }"))
                                 class-names))
                  "\n]\n")]
    (write-archive-text-entry! archive-path native-test-reflect-config body)))

(defn- write-native-test-native-image-properties!
  [archive-path class-names]
  (let [body (str "Args=--initialize-at-run-time="
                  (str/join "," class-names)
                  "\n")]
    (write-archive-text-entry! archive-path native-test-native-image-properties body)))

(defn clean [_]
  (b/delete {:path "target"}))

(defn- delete-zip-entry!
  [archive-path entry-path]
  (let [archive-uri (URI/create (str "jar:" (.toURI (io/file archive-path))))
        link-opts   (into-array LinkOption [])
        path-parts  (into-array String [])]
    (with-open [fs (FileSystems/newFileSystem archive-uri {"create" "false"})]
      (let [entry (.getPath fs (str "/" entry-path) path-parts)]
        (when (Files/exists entry link-opts)
          (Files/delete entry))))))

(defn uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src" "resources"] :target-dir class-dir})
  (b/compile-clj {:basis @basis
                  :ns-compile '[xia.core]
                  :class-dir class-dir
                  :bindings compiler-bindings})
  (b/uber {:class-dir class-dir :uber-file uber-file :basis @basis
           :main 'xia.core}))

(defn native-uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src" "resources"] :target-dir class-dir})
  (write-native-resource-config! class-dir)
  (b/compile-clj {:basis @basis
                  :ns-compile '[xia.core]
                  :class-dir class-dir
                  :bindings compiler-bindings})
  (b/uber {:class-dir class-dir :uber-file uber-file :basis @basis
           :main 'xia.core}))

(defn native-test-uber [_]
  (clean nil)
  (let [selected-test-nss (native-test-namespace-symbols)
        support-nss       (test-support-namespace-symbols)
        closure-nss       (static-native-test-compile-namespaces selected-test-nss)
        compile-nss       (vec (distinct (concat native-test-dependency-namespaces
                                                 closure-nss
                                                 support-nss
                                                 ['xia.native-test-main])))]
    (copy-native-test-sources! selected-test-nss)
    (write-native-resource-config! test-class-dir ["fixtures/"])
    (b/compile-clj {:basis @test-basis
                    :ns-compile compile-nss
                    :class-dir test-class-dir
                    :bindings compiler-bindings})
    (b/uber {:class-dir test-class-dir
             :uber-file test-uber-file
             :basis @test-basis
             :main 'xia.native-test-main})))
