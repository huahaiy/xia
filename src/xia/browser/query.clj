(ns xia.browser.query
  "Shared browser element-query helpers."
  (:require [clojure.string :as str]))

(def default-kind :interactive)
(def default-limit 25)
(def max-limit 200)
(def default-link-preview-limit 20)

(def ^:private kind-aliases
  {:interactive :interactive
   :link :links
   :links :links
   :button :buttons
   :buttons :buttons
   :form :forms
   :forms :forms
   :field :fields
   :fields :fields
   :image :images
   :images :images
   :heading :headings
   :headings :headings
   :table :tables
   :tables :tables
   :all :all})

(def ^:private kind-selectors
  {:interactive "a[href], button, input[type=button], input[type=submit], input[type=reset], input[type=image], [role=button], select, textarea, summary"
   :links "a[href]"
   :buttons "button, input[type=button], input[type=submit], input[type=reset], input[type=image], [role=button]"
   :forms "form"
   :fields "input, textarea, select, button"
   :images "img"
   :headings "h1, h2, h3, h4, h5, h6"
   :tables "table"
   :all "*"})

(defn normalize-kind
  [kind]
  (let [kind* (cond
                (nil? kind) default-kind
                (keyword? kind) kind
                (string? kind) (keyword (str/lower-case kind))
                :else (throw (ex-info "Browser query kind must be a keyword or string"
                                      {:kind kind})))]
    (or (get kind-aliases kind*)
        (throw (ex-info "Unsupported browser query kind"
                        {:kind kind
                         :supported-kinds (sort (map name (keys kind-selectors)))})))))

(defn kind-selector
  [kind]
  (get kind-selectors (normalize-kind kind)))

(defn normalize-opts
  [{:keys [kind selector text-contains visible-only offset limit]}]
  (let [kind* (normalize-kind kind)
        offset* (long (clojure.core/max 0 (long (or offset 0))))
        limit* (long (-> (long (or limit default-limit))
                         (clojure.core/max 1)
                         (clojure.core/min (long max-limit))))
        text* (some-> text-contains str str/trim not-empty)]
    {:kind kind*
     :selector (or (some-> selector str str/trim not-empty)
                   (kind-selector kind*))
     :text-contains text*
     :visible-only (boolean visible-only)
     :offset offset*
     :limit limit*}))

(defn text-matches?
  [needle & haystacks]
  (if-let [needle* (some-> needle str/lower-case not-empty)]
    (boolean
      (some (fn [hay]
              (when-let [hay* (some-> hay str str/lower-case not-empty)]
                (str/includes? hay* needle*)))
            haystacks))
    true))

(defn css-attr-escape
  [value]
  (-> (str value)
      (str/replace "\\" "\\\\")
      (str/replace "\"" "\\\"")))

(defn attr-selector
  [attr value]
  (str "[" attr "=\"" (css-attr-escape value) "\"]"))
