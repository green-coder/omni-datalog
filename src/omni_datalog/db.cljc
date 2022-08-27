(ns omni-datalog.db
  (:require [com.rpl.specter :as sp]
            [malli.core :as m]))

;; WIP namespace, for REPL experimentation

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

(def schema
  [:map
   [:person/id [:map-of int? [:map
                              [:name [:map
                                      [:first string?]
                                      [:last string?]]]
                              [:items [:vector [:map
                                                [:name string?]
                                                [:color string?]]]]]]]
   [:room/id [:map-of int? [:map
                            [:name string?]
                            [:items [:vector [:map
                                              [:name string?]
                                              [:color string?]]]]]]]])

#_(m/validate schema db)

(def nav
  {:root              {nil []}
   :person-entry      {:root [:person/id sp/ALL]}
   :person/id         {:person-entry [sp/FIRST]}
   :person-val        {:person-entry [sp/LAST]}
   :person/first-name {:person-val [:name :first]}
   :person/last-name  {:person-val [:name :last]}
   :person/item       {:person-val [:items sp/ALL]}
   :room-entry        {:root [:room/id sp/ALL]}
   :room/id           {:room-entry [sp/FIRST]}
   :room-val          {:room-entry [sp/LAST]}
   :room/item         {:room-val [:items sp/ALL]}
   :item              {:person/item []
                       :room/item []}
   :item/name         {:item [:name]}
   :item/color        {:item [:color]}})

;; TODO: return a relation tree instead of a relation vector.
;; Maybe using pathom's EQL syntax.
;; TODO: Can we select and append-join with multiple paths using specter?
(defn relations-to [nav relation-kw]
  (let [[parent-kw _] (-> relation-kw nav first)]
    (if (nil? parent-kw)
      [relation-kw]
      (conj (relations-to nav parent-kw) relation-kw))))

#_(relations-to nav :item/color)

;; TODO: this can be derived from the relation-tree
(defn path-to [nav relation-kw]
  (let [[parent-kw path] (-> relation-kw nav first)]
    (if (nil? parent-kw)
      path
      (into (path-to nav parent-kw) path))))

#_(sp/select (path-to nav :person/id) db)
#_(sp/select (path-to nav :person/first-name) db)
#_(sp/select (path-to nav :item/color) db)
