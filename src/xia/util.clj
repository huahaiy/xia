(ns xia.util
  "Small shared helpers used across Xia.")

(defn long-max
  ^long [^long a ^long b]
  (if (> a b) a b))

(defn long-min
  ^long [^long a ^long b]
  (if (< a b) a b))
