(ns omni-datalog.core
  (:require [com.rpl.specter :as sp]
            [malli.core :as m]
            [omni-datalog.db :refer [db]]
            [omni-datalog.query :refer [parse-query]]))

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

(defn select-columns [table & columns]
  (mapv (fn [row]
          (mapv row columns))
        table))

(defn q [query resolvers db & inputs])

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
           :person/item (fn [db]
                          (->> (get-person-entities db)
                               (mapcat (fn [person-entity]
                                         (mapv (fn [item-index]
                                                 [person-entity (conj person-entity :items item-index)])
                                               (-> db
                                                   (get-in person-entity)
                                                   :items
                                                   count
                                                   range))))))
           :item/name (fn [db]
                        (concat
                          (->> (get-person-item-entities db)
                               (mapv (fn [person-item-entity]
                                       [person-item-entity
                                        (-> db
                                            (get-in person-item-entity)
                                            :name)])))
                          (->> (get-room-item-entities db)
                               (mapv (fn [room-item-entity]
                                       [room-item-entity
                                        (-> db
                                            (get-in room-item-entity)
                                            :name)])))))
           :item/color (fn [db]
                         (concat
                           (->> (get-person-item-entities db)
                                (mapv (fn [person-item-entity]
                                        [person-item-entity
                                         (-> db
                                             (get-in person-item-entity)
                                             :color)])))
                           (->> (get-room-item-entities db)
                                (mapv (fn [room-item-entity]
                                        [room-item-entity
                                         (-> db
                                             (get-in room-item-entity)
                                             :color)])))))}})

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
      (select-columns 1 5)))
