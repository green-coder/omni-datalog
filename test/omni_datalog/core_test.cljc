(ns omni-datalog.core-test
  (:require [clojure.test :refer [deftest testing is are]]
            [com.rpl.specter :as sp]
            [omni-datalog.core :as o]))

(def db
  {:person/id {0 {:name {:first "Alice"
                         :last "A-name"}
                  :items [{:name "apple"
                           :color "red"}
                          {:name "rose"
                           :color "white"}]}
               1 {:name {:first "Bob"
                         :last "B-name"}
                  :items [{:name "letter"
                           :color "pink"}
                          {:name "ball"
                           :color "white"}]}}
   :room/id {0 {:name "living room"
                :items [{:name "plant"
                         :color "green"}
                        {:name "wii"
                         :color "white"}]}}})

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





(deftest parse-query-test
  (is (= '{:find [?person-id ?item-name],
           :in [[?item-color]],
           :where [[?p :person/id ?person-id]
                   [?p :person/item ?i]
                   [?i :item/name ?item-name]
                   [?i :item/color ?item-color]]}
         (o/parse-query '[:find ?person-id ?item-name
                          :in $ ?item-color
                          :where
                          [?p :person/id ?person-id]
                          [?p :person/item ?i]
                          [?i :item/name ?item-name]
                          [?i :item/color ?item-color]]))))

(deftest extract-rows-from-db-test
  (is (= [[[:person/id 0] 0] [[:person/id 1] 1]]
         (#'o/extract-rows-from-db resolvers db '[?p :person/id ?person-id])))
  (is (= [[[:person/id 0] "Alice"] [[:person/id 1] "Bob"]]
         (#'o/extract-rows-from-db resolvers db '[?p :person/first-name ?person-first-name]))))

(deftest common-columns-indexes-test
  (is (= [[0 3] [3 2]]
         (#'o/common-columns-indexes '[x a b y] '[c d y x])))
  (is (= [[] []]
         (#'o/common-columns-indexes '[a b c] '[d e f])))
  (is (= [[0 1 2] [0 1 2]]
         (#'o/common-columns-indexes '[a b c] '[a b c])))
  (is (= [[0 1 2] [1 2 0]]
         (#'o/common-columns-indexes '[a b c] '[c a b]))))

(deftest inner-join*-test
  (is (= '[[a1 b1 c1 c1 d1]
           [a1 b1 c1 c1 d2]
           [a2 b2 c2 c2 d3]]
         (#'o/inner-join* '[[a1 b1 c1]
                            [a2 b2 c2]]
                          '[2]
                          '[[c1 d1]
                            [c1 d2]
                            [c2 d3]
                            [c3 d4]]
                          '[0])))
  (is (= '[[a1 b1 c1 c1 d1]
           [a1 b1 c1 c1 d2]
           [a1 b1 c1 c2 d3]
           [a1 b1 c1 c3 d4]
           [a2 b2 c2 c1 d1]
           [a2 b2 c2 c1 d2]
           [a2 b2 c2 c2 d3]
           [a2 b2 c2 c3 d4]]
         (#'o/inner-join* '[[a1 b1 c1]
                            [a2 b2 c2]]
                          '[]
                          '[[c1 d1]
                            [c1 d2]
                            [c2 d3]
                            [c3 d4]]
                          '[]))))

(deftest q-test
  (is (= [[0 "rose"] [1 "ball"]]
         (o/q '[:find ?person-id ?item-name
                :in $ ?item-color
                :where
                [?p :person/id ?person-id]
                [?p :person/item ?i]
                [?i :item/name ?item-name]
                [?i :item/color ?item-color]]
               resolvers
               db
               [["white"]])))
  (is (= [[0 "Alice" "rose"] [1 "Bob" "ball"]]
         (o/q '[:find ?person-id ?person-first-name ?item-name
                :in $ ?item-color
                :where
                [?p :person/id ?person-id]
                [?p :person/first-name ?person-first-name]
                [?p :person/item ?i]
                [?i :item/name ?item-name]
                [?i :item/color ?item-color]]
               resolvers
               db
               [["white"]])))
  ;; BUG !!!
  #_
  (is (= [[0 "Alice" "rose"]]
         (o/q '[:find ?person-id ?person-first-name ?item-name
                :in $ [?item-name ?item-color]
                :where
                [?p :person/id ?person-id]
                [?p :person/first-name ?person-first-name]
                [?p :person/item ?i]
                [?i :item/name ?item-name]
                [?i :item/color ?item-color]]
               resolvers
               db
               [["rose" "white"]]))))
