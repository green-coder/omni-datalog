(ns omni-datalog.core
  (:require [clojure.set :as set]
            [com.rpl.specter :as sp]
            [omni-datalog.db :refer [db]]
            [omni-datalog.query :refer [parse-query query inputs]]))

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

(defn select-columns [table columns]
  (mapv (fn [row]
          (mapv row columns))
        table))

(defn- extract-table-from-db [resolvers db rule]
  (let [[e a v] rule
        resolver (-> resolvers :a->ev a)]
    (resolver db)))

#_(extract-table-from-db resolvers db '[?p :person/id ?person-id])

(defn table-join-indexes [columns1 columns2]
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

(defn join-tables
  ([] nil)
  ([[columns rows]] [columns rows])
  ([[columns1 rows1] [columns2 rows2]]
   (let [[column-index1 column-index2] (table-join-indexes columns1 columns2)]
     [(into columns1 columns2)
      (full-join rows1 column-index1 rows2 column-index2)])))

(defn q [query resolvers db & inputs]
  (let [{:keys [find in where]} (parse-query query)
        tables (mapv (fn [[e a v :as rule]]
                       ;; [columns rows]
                       [[e v] (extract-table-from-db resolvers db rule)])
                     where)
        tables (into tables
                     (map-indexed (fn [index input]
                                    [[(nth in (inc index))] [[input]]]))
                     inputs)
        [result-columns results-rows] (reduce join-tables tables)
        result-column->index (into {} (map-indexed (fn [index symb] [symb index])) result-columns)
        projection-indexes (mapv result-column->index find)]
    (select-columns results-rows projection-indexes)))

;;;;;;;;;;;;;;;; Example of usage ;;;;;;;;;;;;;;;;

(defn get-person-entities [db]
  (sp/select [:person/id (sp/putval :person/id) sp/MAP-KEYS] db))

(defn get-person-item-entities [db]
  (sp/select [:person/id (sp/putval :person/id) sp/ALL (sp/collect-one sp/FIRST) sp/LAST :items (sp/putval :items) sp/INDEXED-VALS sp/FIRST] db))

(defn get-room-item-entities [db]
  (sp/select [:room/id (sp/putval :room/id) sp/ALL (sp/collect-one sp/FIRST) sp/LAST :items (sp/putval :items) sp/INDEXED-VALS sp/FIRST] db))

;; Most naive approach, where the query is resolved via full joins between tables.
(def resolvers
  {:a->ev {:person/id (fn [db]
                        (->> (get-person-entities db)
                             (mapv (fn [person-entity]
                                     [person-entity (peek person-entity)]))))
           :person/first-name (fn [db]
                                (->> (get-person-entities db)
                                     (mapv (fn [person-entity]
                                             [person-entity (-> (get-in db person-entity)
                                                                :name
                                                                :first)]))))
           :person/item (fn [db]
                          (->> (get-person-entities db)
                               (into []
                                     (mapcat (fn [person-entity]
                                               (mapv (fn [item-index]
                                                       [person-entity (conj person-entity :items item-index)])
                                                     (-> db
                                                         (get-in person-entity)
                                                         :items
                                                         count
                                                         range)))))))
           :item/name (fn [db]
                        (-> []
                            (into (->> (get-person-item-entities db)
                                       (mapv (fn [person-item-entity]
                                               [person-item-entity
                                                (-> db
                                                    (get-in person-item-entity)
                                                    :name)]))))
                            (into (->> (get-room-item-entities db)
                                       (mapv (fn [room-item-entity]
                                               [room-item-entity
                                                (-> db
                                                    (get-in room-item-entity)
                                                    :name)]))))))
           :item/color (fn [db]
                         (-> []
                             (into (->> (get-person-item-entities db)
                                        (mapv (fn [person-item-entity]
                                                [person-item-entity
                                                 (-> db
                                                     (get-in person-item-entity)
                                                     :color)]))))
                             (into (->> (get-room-item-entities db)
                                        (mapv (fn [room-item-entity]
                                                [room-item-entity
                                                 (-> db
                                                     (get-in room-item-entity)
                                                     :color)]))))))}})

;; Resolves the query "by hand"
(let [table1 ((-> resolvers :a->ev :person/id) db)
      table2 ((-> resolvers :a->ev :person/item) db)
      table3 ((-> resolvers :a->ev :item/name) db)
      table4 ((-> resolvers :a->ev :item/color) db)
      table-input1 [["white"]]]
  (-> table1
      (full-join 0 table2 0)
      (full-join 3 table3 0)
      (full-join 3 table4 0)
      (full-join 7 table-input1 0)
      (select-columns [1 5])))

;; Resolves the query in a more automatic way
(q '[:find ?person-id ?item-name
     :in $ ?item-color
     :where
     [?p :person/id ?person-id]
     [?p :person/item ?i]
     [?i :item/name ?item-name]
     [?i :item/color ?item-color]]
   resolvers
   db
   "white")

;; Another test
(q '[:find ?person-id ?person-first-name ?item-name
     :in $ ?item-color
     :where
     [?p :person/id ?person-id]
     [?p :person/first-name ?person-first-name]
     [?p :person/item ?i]
     [?i :item/name ?item-name]
     [?i :item/color ?item-color]]
   resolvers
   db
   "white")
