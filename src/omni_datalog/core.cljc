(ns omni-datalog.core
  (:require [clojure.string :as str]
            [mate.core :as mc]))

(defrecord Relation [columns rows])

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

(defn- make-item->index [coll]
  (into {}
        (map-indexed (fn [index item] [item index]))
        coll))

(defn- find-index [item coll]
  ((make-item->index coll) item))

(defn- select-columns
  "Returns a relation with only the selected columns."
  [relation selection-columns]
  (let [rel-column->index (make-item->index (:columns relation))
        selection-indexes (mapv rel-column->index selection-columns)]
    (->Relation selection-columns
                (mapv (fn [row]
                        (mapv row selection-indexes))
                      (:rows relation)))))

(defn- extract-rows-from-db
  "Returns a sequence of rows."
  [resolvers db rule]
  (let [[e a v] rule
        resolver (-> resolvers :a->ev a)]
    (resolver db)))

(defn- common-columns-indexes
  "Returns the indexes of columns on which to perform a join."
  [columns1 columns2]
  (let [columns->index1 (make-item->index columns1)
        columns->index2 (make-item->index columns2)
        common-columns  (filterv columns->index2 columns1)]
    [(mapv columns->index1 common-columns)
     (mapv columns->index2 common-columns)]))

(defn- inner-join
  "Joins 2 relations based on columns sharing the same name."
  ([] nil)
  ([relation] relation)
  ([relation1 relation2]
   (let [columns1 (:columns relation1)
         columns2 (:columns relation2)
         [column-indexes1 column-indexes2] (common-columns-indexes columns1 columns2)
         result-indexes2 (into [] (remove (set column-indexes2)) (range (count columns2)))
         join-columns->rows1 (group-by (fn [row] (mapv row column-indexes1)) (:rows relation1))
         join-columns->rows2 (group-by (fn [row] (mapv row column-indexes2)) (:rows relation2))]
     (->Relation (into columns1 (mapv columns2 result-indexes2))
                 (-> (for [join-value  (keys join-columns->rows1)
                           :let [result-rows1 (join-columns->rows1 join-value)
                                 result-rows2 (cond->> (join-columns->rows2 join-value)
                                                       (seq column-indexes2)
                                                       (mapv (fn [row]
                                                               (mapv row result-indexes2))))]
                           left-row-part result-rows1
                           right-row-part result-rows2]
                       (into left-row-part right-row-part))
                     vec)))))

;; To be considered as a small implementation optimisation, later:
;;   - e->a? (0 or 1)
;;   - ea->v* (0 or more)

(defn a->ev [resolvers resolver-id db
             entity-column value-column]
  (let [resolver (-> resolvers :a->ev resolver-id)]
    (->Relation [entity-column value-column]
                (resolver db))))

;; Not important at the moment
#_(defn a->e [resolvers resolver-id db
              entity-column]
    (let [resolver (-> resolvers :a->e resolver-id)]
      (->Relation [entity-column]
                  (mapv vector (resolver db)))))

(defn ea->v [resolvers resolver-id db
             input-relation entity-column value-column]
  (let [resolver (-> resolvers :ea->v resolver-id)
        entity-column-index (find-index entity-column (:columns input-relation))]
    (->Relation (conj (:columns input-relation) value-column)
                (-> (for [row (:rows input-relation)
                          :let [entity (row entity-column-index)]
                          value (resolver db entity)]
                      (conj row value))
                    vec))))

(defn av->e [resolvers resolver-id db
             input-relation value-column entity-column]
  (let [resolver (-> resolvers :av->e resolver-id)
        value-column-index (find-index value-column (:columns input-relation))]
    (->Relation (conj (:columns input-relation) entity-column)
                (-> (for [row (:rows input-relation)
                          :let [value (row value-column-index)]
                          entity (resolver db value)]
                      (conj row entity))
                    vec))))

(defn- variable? [x]
  (and (symbol? x) (str/starts-with? (name x) "?")))

(defn sanitize-query
  "Returns a parsed query which only keeps:
   1. The variables in :find
   2. The rules which are transitively related to the variables in :find
   3. The inputs referenced in those rules"
  [parsed-query]
  (let [{:keys [find in where]} parsed-query
        find-set (->> (flatten find)
                      (filter variable?)
                      (set))
        rule->vars (into {}
                         (map (fn [rule]
                                [rule (filterv variable? (flatten rule))]))
                         where)
        var->rules (->> where
                        (mapcat (fn [rule]
                                  (->> (rule->vars rule)
                                       (mapv (fn [rule-element]
                                               [rule-element rule])))))
                        (filter (fn [[rule-element _rule]] (variable? rule-element)))
                        (mc/group-by first second))
        [searched-vars required-rules] (loop [vars-to-search find-set
                                              searched-vars #{}
                                              required-rules #{}]
                                         (if (empty? vars-to-search)
                                           [searched-vars required-rules]
                                           (let [new-rules (mapcat var->rules vars-to-search)
                                                 searched-vars (into searched-vars vars-to-search)
                                                 required-rules (into required-rules new-rules)
                                                 new-vars-to-search (into #{}
                                                                          (comp (mapcat rule->vars)
                                                                                (remove searched-vars))
                                                                          new-rules)]
                                             (recur new-vars-to-search
                                                    searched-vars
                                                    required-rules))))]
    {:find find
     :in (filterv (fn [input]
                    (->> (flatten input)
                         (filter variable?)
                         (some searched-vars)))
                  in)
     :where (filterv required-rules where)}))

(defn q
  "Resolves a Datalog query."
  [query resolvers db & inputs]
  (let [{:keys [find in where]} (parse-query query)
        relations (mapv (fn [[e a v :as rule]]
                          (->Relation [e v]
                                      (extract-rows-from-db resolvers db rule)))
                        where)
        relations (into relations
                        (map (fn [in-columns in-rows]
                               (->Relation in-columns in-rows))
                             in
                             inputs))
        result-relation (reduce inner-join relations)]
    (:rows (select-columns result-relation find))))

;; Improvements in the reference version:
;; [x] 1. Consecutive rules may not be related. inner-join on them might be wrong or too early.
;; [x] 2. Support more format for the :in and the inputs parameters.
;; [x]   - [?item-name ?item-color]
;; [x]   - accept row sets for the inputs
;; [x] 3. Utility function `make-item->index`
;; [ ] 4. Wrong assumption about the patterns of the rules, where only the attribute is known (a->ev).
;; [ ]   - Identify the columns already known in a new rule to process.
;;       - Support resolver formats:
;;         - Important ones:
;; [x]       - a->ev (a.k.a. the relation builder)
;; [x]       - ea->v (a.k.a. the relation expander)
;; [x]       - av->e (a.k.a. the reverse relation expander)
;;         - Rarely needed / not sure to support them
;; [ ]       - e->av
;; [ ]       - ->eav
;; [ ]       - ev->a
;; [ ]       - v->ea
;;         - Those can be derived from the above:
;; [ ]       - e->a
;; [ ]       - e->v
;; [ ]       - a->e
;; [ ]       - a->v
;; [ ]       - v->e
;; [ ]       - v->a
;; [ ]   - Support other ways than to compose relations using those resolver formats.
;; [ ]   - (just for convenience) Recognize the constants in the rule's e-a-v triple.
;;         - A constant entity is a vector literal representing a db path.
;;         - A constant attribute is a keyword.
;;         - A constant value is anything but a symbol which starts with a "?". `^:value` tag can be used if needed.
;; [x] 5. Wrong assumption about the number of common-columns between 2 given relations to join. Not always 1.
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
;; - Each relation is a rule applied to the db
;; - When we join relations, we just group rules inside a (and rule1 rule2)
;; - Joining all the relations together means putting all the rules inside a (and ...)
;; - Selecting which relations to join means selecting which 2 rules to group.
