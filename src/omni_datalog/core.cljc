(ns omni-datalog.core)

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

(defn- select-columns
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

(defn- make-item->index [coll]
  (into {}
        (map-indexed (fn [index item] [item index]))
        coll))

(defn common-columns-indexes
  "Returns the indexes of columns on which to perform a table join."
  [columns1 columns2]
  (let [columns->index1 (make-item->index columns1)
        columns->index2 (make-item->index columns2)
        common-columns  (filterv columns->index2 columns1)]
    [(mapv columns->index1 common-columns)
     (mapv columns->index2 common-columns)]))

#_(common-columns-indexes '[x a b y] '[c d y x])

;; Inner join between tables 1 and 2
(defn- inner-join*
  "Returns the inner join between two tables."
  [rows1 column-indexes1 rows2 column-indexes2]
  (let [xxx1 (group-by (fn [row] (mapv row column-indexes1)) rows1)
        xxx2 (group-by (fn [row] (mapv row column-indexes2)) rows2)]
    (-> (for [join-value (keys xxx1)
              val1 (xxx1 join-value)
              val2 (xxx2 join-value)]
          (into val1 val2))
        vec)))

(defn inner-join-tables
  "Joins 2 tables based on columns sharing the same name."
  ([] nil)
  ([[rows columns]] [rows columns])
  ([[rows1 columns1] [rows2 columns2]]
   (let [[column-indexes1 column-indexes2] (common-columns-indexes columns1 columns2)]
     ;; FIXME: get rid of redundant columns.
     [(inner-join* rows1 column-indexes1 rows2 column-indexes2)
      (into columns1 columns2)])))

(defn q
  "Resolves a Datalog query."
  [query resolvers db & inputs]
  (let [{:keys [find in where]} (parse-query query)
        tables (mapv (fn [[e a v :as rule]]
                       ;; [rows columns]
                       [(extract-rows-from-db resolvers db rule) [e v]])
                     where)
        tables (into tables
                     (map (fn [in-rows in-columns]
                            [in-rows in-columns])
                          inputs
                          in))
        [results-rows result-columns] (reduce inner-join-tables tables)
        result-column->index (make-item->index result-columns)
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
        (inner-join* [0] rows2 [0])
        (inner-join* [3] rows3 [0])
        (inner-join* [3] rows4 [0])
        (inner-join* [7] rows-input1 [0])
        (select-columns [1 5])))
  ,)

;; [ ] 1. Consecutive rules may not be related. inner-join on them might be wrong or too early.
;; [x] 2. Support more format for the :in nd the inputs parameters.
;; [x]  - [?item-name ?item-color]
;; [x]  - accept row sets for the inputs
;; [x] 3. Utility function `make-item->index`
;; [ ] 4. Wrong assumption about the patterns of the rules, where only the attribute is known.
;; [x] 5. Wrong assumption about the number of common-columns between 2 given tables to join. Not always 1.
