(ns falwell.core
  (:import [java.nio.file Files Paths StandardOpenOption])
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [table.core :as table]
            [loom.graph :as graph]
            [clojure.java.shell :as shell]
            [loom.io :as graph-view]))

(defn mapify [[headers & xs]]
  (let [headers (map keyword headers)]
    (map
      #(apply hash-map (interleave headers %))
      xs)))

(defn uniques [kw v] (set (map kw v)))

(defonce data 
  (with-open [reader (io/reader "logs/logs.csv")]
    (doall
      (map (fn [x]
             (-> x
                 (update :recipient_status (comp clojure.string/lower-case 
                                                 first 
                                                 #(clojure.string/split % #"##")))
                 (update :sender_address clojure.string/lower-case)))
           (mapify (csv/read-csv reader))))))

(defonce senders (uniques :sender_address data))

(defonce recipients (uniques :recipient_status data))

(defonce emails (into senders recipients))

(defonce unique-domains
  (set (map (comp second #(clojure.string/split % #"@")) emails)))

(defonce domain-frequencies
  (reduce (fn [acc x] (update acc x (fnil inc 0)))
          (sorted-map)
          (map (comp second #(clojure.string/split % #"@")) 
               emails)))

(defn table [data & headers]
  (table/table-str (into [headers] data) :style :unicode))

(defonce edges 
  (sort-by first
           (sort-by second
                    (map (fn [{:keys [sender_address recipient_status]}] 
                           [sender_address recipient_status])
                         data))))

(defonce weighted-edges 
  (map (fn [[x :as l]] (conj x (count l)))
       (partition-by (partial apply str)
                     edges)))

(defonce weighted-digraph (apply loom.graph/weighted-digraph weighted-edges))

(defn dump-bytes [f ext b]
  (let [p (Paths/get (str f "." ext) (into-array String []))]
    (Files/write p 
                 b
                 (into-array StandardOpenOption [StandardOpenOption/WRITE StandardOpenOption/CREATE]))))

(defn render-to-bytes [g alg fmt]
  (:out
    (shell/sh alg
              (str "-T" fmt)
              #_"-Goverlap=scale"
              "-Nshape=point"
              :in (graph-view/dot-str g :alg alg :fmt fmt)
              :out-enc :bytes)))

(defn dump-graph [f g alg fmt]
  (dump-bytes f fmt (render-to-bytes g alg fmt)))

(defn -main [& args])
