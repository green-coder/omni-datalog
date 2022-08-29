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

(defn- common-columns-indexes
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
  [rows1 column-indexes1 rows2 column-indexes2 columns2-count]
  (let [result-indexes2 (into [] (remove (set column-indexes2)) (range columns2-count))
        join-columns->rows1 (group-by (fn [row] (mapv row column-indexes1)) rows1)
        join-columns->rows2 (group-by (fn [row] (mapv row column-indexes2)) rows2)]
    (-> (for [join-value  (keys join-columns->rows1)
              :let [result-rows1 (join-columns->rows1 join-value)
                    result-rows2 (cond->> (join-columns->rows2 join-value)
                                          (seq column-indexes2)
                                          (mapv (fn [row]
                                                  (mapv row result-indexes2))))]
              left-row-part result-rows1
              right-row-part result-rows2]
          (into left-row-part right-row-part))
        vec)))

(defn inner-join-tables
  "Joins 2 tables based on columns sharing the same name."
  ([] nil)
  ([[rows columns]] [rows columns])
  ([[rows1 columns1] [rows2 columns2]]
   (let [[column-indexes1 column-indexes2] (common-columns-indexes columns1 columns2)
         result-indexes2 (into [] (remove (set column-indexes2)) (range (count columns2)))]
     [(inner-join* rows1 column-indexes1 rows2 column-indexes2 (count columns2))
      (into columns1 (mapv columns2 result-indexes2))])))

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

;; Improvements in the reference version:
;; [x] 1. Consecutive rules may not be related. inner-join on them might be wrong or too early.
;; [x] 2. Support more format for the :in and the inputs parameters.
;; [x]   - [?item-name ?item-color]
;; [x]   - accept row sets for the inputs
;; [x] 3. Utility function `make-item->index`
;; [ ] 4. Wrong assumption about the patterns of the rules, where only the attribute is known (a->ev).
;; [ ]   - Identify the columns already known in a new rule to process.
;; [ ]   - Support resolver formats:
;;         - ->eav,
;;         - e->av, a->ev (done), v->ea, av->e, ev->a, ea->v
;;         - e->a, e->v, a->e, a->v, v->e, v->a
;; [ ]   - Support other ways than to compose tables using those resolver formats.
;; [ ]   - (just for convenience) Recognize the constants in the rule's e-a-v triple.
;;         - A constant entity is a vector literal representing a db path.
;;         - A constant attribute is a keyword.
;;         - A constant value is anything but a symbol which starts with a "?". `^:value` tag can be used if needed.
;; [x] 5. Wrong assumption about the number of common-columns between 2 given tables to join. Not always 1.
;; [x] 6. Remove duplicated columns during inner joins.
;; [ ] 7. Support "filter" functions in rules.
;; [ ] 8. Support "transform" functions in rules (one-to-one, one-to-many).
;; [ ] 9. Support a way to bind to either collection values or the individual items inside.
;;       - [?person :person/items ?items] vs [?person :person/item ?item]
;;         - doable by using additional resolvers
;;       - (convenience) [?person :person/items ?items] vs [?person :person/items [?item]], same as:
;;         - [?person :person/items ?items]
;;         - [(elements-of ?items) ?item]
;;       - (convenience) [?person :person/name {?first :person.name/first, ?last :person.name/last}], same as:
;;         - [?person :person/name ?name]
;;         - [?name :person.name/first ?first]
;;         - [?name :person.name/last ?last]


;; Note:
;; - Each table is a rule applied to the db
;; - When we join tables, we just group rules inside a (and rule1 rule2)
;; - Joining all the tables together means putting all the rules inside a (and ...)
;; - Selecting which tables to join means selecting which 2 rules to group.
