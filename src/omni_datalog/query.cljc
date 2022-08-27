(ns omni-datalog.query)

(comment

  ;; Ideas: it looks like the datalog queries could be based on pull queries.
  ;; If at some point we can be generic enough, it means that we could be based on pathom !!!
  ;; networked datalog queries in sight - WOOT!!

  (q '[:find ?person-id ?item-name
       :in $ ?item-color
       :where
       [?p :person/id ?person-id]
       [?p :person/item ?i]
       [?i :item/name ?item-name]
       [?i :item/color ?item-color]]
     "white")

  ,)

(defn parse-query [query]
  (into {}
        (comp (partition-by #{:find :in :where})
              (partition-all 2)
              (map (fn [[[k] vs]]
                     [k vs])))
        query))

(def query
  '[:find ?person-id ?item-name
    :in $ ?item-color
    :where
    [?p :person/id ?person-id]
    [?p :person/item ?i]
    [?i :item/name ?item-name]
    [?i :item/color ?item-color]])

#_(parse-query query)
