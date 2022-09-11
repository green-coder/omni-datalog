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
                           :color "white"}]
                  :in-room 0}
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

(def resolvers
  {;; -> [entity ...]
   ;; Not important at the moment
   #_#_
   :a->e {:person-entity get-person-entities}

   ;; -> [entity ...]
   :av->e {:item/color (fn [db item-color]
                         (into []
                               (filter (fn [item-entity]
                                         (let [item (get-in db item-entity)]
                                           (and (contains? item :color)
                                                (= (:color item) item-color)))))
                               (concat (get-person-item-entities db)
                                       (get-room-item-entities db))))}

   ;; -> [value ...]
   :ea->v {:person/id (fn [_db person-entity]
                        (when-some [[_:person|id person-id] person-entity]
                          [person-id]))
           :person/first-name (fn [db person-entity]
                                (let [name (-> (get-in db person-entity) :name)]
                                  (when (contains? name :first)
                                    [(:first name)])))
           :person/last-name (fn [db person-entity]
                               (let [name (-> (get-in db person-entity) :name)]
                                 (when (contains? name :last)
                                   [(:last name)])))
           :item/name (fn [db item-entity]
                        (let [item (get-in db item-entity)]
                          (when (contains? item :name)
                            [(:name item)])))}

   ;; -> [[entity value] ...]
   :a->ev {:person/id (fn [db]
                        (->> (get-person-entities db)
                             (mapv (fn [person-entity]
                                     [person-entity (peek person-entity)]))))
           :person/first-name (fn [db]
                                (->> (get-person-entities db)
                                     (mapv (fn [person-entity]
                                             [person-entity (-> (get-in db person-entity)
                                                                :name
                                                                :first)]))))
           :person/last-name (fn [db]
                               (->> (get-person-entities db)
                                    (mapv (fn [person-entity]
                                            [person-entity (-> (get-in db person-entity)
                                                               :name
                                                               :last)]))))
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


(comment
  ;; Datalog query
  (o/q '[:find ?person-id ?item-name
         :in $ ?item-color
         :where
         [?p :person/id ?person-id]
         [?p :person/item ?i]
         [?i :item/name ?item-name]
         [?i :item/color ?item-color]]
       resolvers
       db
       [["white"]])
  ;; => [[0 "rose"] [1 "ball"]]

  ;; This resolves the query "by hand" using only inner joins between relations.
  (let [rel1        (o/a->ev resolvers :person/id db '?p '?person-id)
        rel2        (o/a->ev resolvers :person/item db '?p '?i)
        rel3        (o/a->ev resolvers :item/name db '?i '?item-name)
        rel4        (o/a->ev resolvers :item/color db '?i '?item-color)
        input-rel1  (o/->Relation '[?item-color] [["white"]])]
    (-> rel1
        (#'o/inner-join rel2)
        (#'o/inner-join rel3)
        (#'o/inner-join rel4)
        (#'o/inner-join input-rel1)
        (#'o/select-columns '[?person-id ?item-name])
        :rows))
  ;; => [[0 "rose"] [1 "ball"]]

  ,)


(comment
  ;; Query which can use the implicit indexes of the db
  (o/q '[:find ?first-name ?last-name
         :where
         [?p :person/first-name ?first-name]
         [?p :person/last-name ?last-name]]
       resolvers
       db)
  ;; => [["Alice" "A-name"] ["Bob" "B-name"]]

  ;; Resolved by hand
  (let [rel1 (o/a->ev resolvers :person/first-name db '?p '?first-name)
        rel2 (o/ea->v resolvers :person/last-name db rel1 '?p '?last-name)]
    (-> rel2
        (#'o/select-columns '[?first-name ?last-name])
        :rows))
  ;; => [["Alice" "A-name"] ["Bob" "B-name"]]

  ;; relation, resolver-path, rule
  ,)


(comment
  ;; Query which can use a reverse index av->e for [?i :item/color ?item-color]
  (o/q '[:find ?item-name
         :in $ ?item-color
         :where
         [?i :item/color ?item-color]
         [?i :item/name ?item-name]]
       resolvers
       db
       [["white"]])
  ;; => [["rose"] ["ball"] ["wii"]]

  ;; Resolved by hand
  (let [input-rel1 (o/->Relation '[?item-color] [["white"]])
        rel1 (o/av->e resolvers :item/color db input-rel1 '?item-color '?i)
        rel2 (o/ea->v resolvers :item/name db rel1 '?i '?item-name)]
    (-> rel2
        (#'o/select-columns '[?item-name])
        :rows))
  ;; => [["rose"] ["ball"] ["wii"]]

  ,)


;;;;;;;;;;;;;;;;;;;;; Testing ;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

(deftest inner-join-relations-test
  (is (= (o/->Relation '[?a ?b ?c ?d]
                       '[[a1 b1 c1 d1]
                         [a1 b1 c1 d2]
                         [a2 b2 c2 d3]])
         (#'o/inner-join (o/->Relation '[?a ?b ?c]
                                       '[[a1 b1 c1]
                                         [a2 b2 c2]])
                         (o/->Relation '[?c ?d]
                                       '[[c1 d1]
                                         [c1 d2]
                                         [c2 d3]
                                         [c3 d4]]))))
  (is (= (o/->Relation '[?a ?b ?x ?y ?d]
                       '[[a1 b1 c1 c1 d1]
                         [a1 b1 c1 c1 d2]
                         [a1 b1 c1 c2 d3]
                         [a1 b1 c1 c3 d4]
                         [a2 b2 c2 c1 d1]
                         [a2 b2 c2 c1 d2]
                         [a2 b2 c2 c2 d3]
                         [a2 b2 c2 c3 d4]])
         (#'o/inner-join (o/->Relation '[?a ?b ?x]
                                       '[[a1 b1 c1]
                                         [a2 b2 c2]])
                         (o/->Relation '[?y ?d]
                                       '[[c1 d1]
                                         [c1 d2]
                                         [c2 d3]
                                         [c3 d4]])))))

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
  (testing "Reordering the rules should still work"
    (is (= #{[0 "rose"] [1 "ball"]}
           (set (o/q '[:find ?person-id ?item-name
                       :in $ ?item-color
                       :where
                       [?p :person/id ?person-id]
                       [?i :item/name ?item-name]
                       [?i :item/color ?item-color]
                       [?p :person/item ?i]]
                      resolvers
                      db
                      [["white"]])))))
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
