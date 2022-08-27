(ns omni-datalog.core
  (:require [clojure.set :as set]))

;; Ideas: it looks like the datalog queries could be based on pull queries.
;; If at some point we can be generic enough, it means that we could be based on pathom !!!
;; networked datalog queries in sight - WOOT!!

(defn parse-query
  "Parses the query from a vector-based shape into a hashmap."
  [query]
  (into {}
        (comp (partition-by #{:find :in :where})
              (partition-all 2)
              (map (fn [[[k] vs]]
                     (case k
                       :find [k vs]
                       :in [k (into []
                                    (comp (remove #{'$})
                                          (map (fn [in-columns]
                                                 (if (vector? in-columns)
                                                   in-columns
                                                   [in-columns]))))
                                    vs)]
                       :where [k vs]))))
        query))

;; Full join between tables 1 and 2
(defn full-join
  "Returns the full join between two tables."
  [table1 join-index1 table2 join-index2]
  (let [xxx1 (group-by #(nth % join-index1) table1)
        xxx2 (group-by #(nth % join-index2) table2)]
    (for [join-value (keys xxx1)
          val1 (xxx1 join-value)
          val2 (xxx2 join-value)]
      (into val1 val2))))

(defn select-columns
  "Select parts of the rows, according to column indexes."
  [rows column-indexes]
  (mapv (fn [row]
          (mapv row column-indexes))
        rows))

(defn- extract-rows-from-db
  "Returns a sequence of rows."
  [resolvers db rule]
  (let [[e a v] rule
        resolver (-> resolvers :a->ev a)]
    (resolver db)))

#_(extract-rows-from-db resolvers db '[?p :person/id ?person-id])

(defn table-join-indexes
  "Returns the indexes of columns on which to perform a table join."
  [columns1 columns2]
  ;; FIXME: not always 1. The return format has to be changed.
  (let [c1 (set columns1)
        c2 (set columns2)
        join-column (first (set/intersection c1 c2))]
    [(-> (keep-indexed (fn [index column]
                          (when (= column join-column)
                            index))
                       columns1)
         first)
     (-> (keep-indexed (fn [index column]
                          (when (= column join-column)
                            index))
                       columns2)
         first)]))

#_(table-join-indexes '[a b] '[c a])

(defn inner-join-tables
  "Joins 2 tables based on columns sharing the same name."
  ([] nil)
  ([[columns rows]] [columns rows])
  ([[columns1 rows1] [columns2 rows2]]
   (let [[column-index1 column-index2] (table-join-indexes columns1 columns2)]
     ;; FIXME: get rid of redundant columns.
     [(into columns1 columns2)
      (full-join rows1 column-index1 rows2 column-index2)])))

(defn q
  "Resolves a Datalog query."
  [query resolvers db & inputs]
  (let [{:keys [find in where]} (parse-query query)
        tables (mapv (fn [[e a v :as rule]]
                       ;; [columns rows]
                       [[e v] (extract-rows-from-db resolvers db rule)])
                     where)
        tables (into tables
                     (map (fn [in-columns in-rows]
                            [in-columns in-rows])
                          in
                          inputs))
        [result-columns results-rows] (reduce inner-join-tables tables)
        result-column->index (into {} (map-indexed (fn [index symb] [symb index])) result-columns)
        projection-indexes (mapv result-column->index find)]
    (select-columns results-rows projection-indexes)))

;;;;;;;;;;;;;;;; Example of usage ;;;;;;;;;;;;;;;;

(comment
  ;; Resolves the query "by hand"
  (let [rows1       ((-> resolvers :a->ev :person/id) db)
        rows2       ((-> resolvers :a->ev :person/item) db)
        rows3       ((-> resolvers :a->ev :item/name) db)
        rows4       ((-> resolvers :a->ev :item/color) db)
        rows-input1 [["white"]]]
    (-> rows1
        (full-join 0 rows2 0)
        (full-join 3 rows3 0)
        (full-join 3 rows4 0)
        (full-join 7 rows-input1 0)
        (select-columns [1 5])))
  ,)

;; [ ] 1. Consecutive rules may not be related. inner-join on them might be wrong or too early.
;; [x] 2. Support more format for the :in nd the inputs parameters.
;; [x]  - [?item-name ?item-color]
;; [x]  - accept row sets for the inputs
;; [ ] 3. Utility function `find-index-in-vector`, or `item->index`
;; [ ] 4. Wrong assumption about the patterns of the rules, where only the attribute is known.
;; [ ] 5. Wrong assumption about the number of join-columns between 2 given rules. Not always 1.
