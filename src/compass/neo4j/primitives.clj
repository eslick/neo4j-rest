(ns com.compass.neo4j.primitives
  (:use
   [com.ianeslick.neo4j.rest])
  (:require
   [clojure.contrib.string :as string]))


;; A Node
;; - has Properties = implements Map protocol, with type checking
;; - has Relationships = implements Node protocol

;; - Manipulate like a map
;; - Save to 

;; A Relationship
;; - (start-node rel)
;; - (end-node rel)
;; - (relation-type rel)
;; - has Properties = implements Map protocol

;; Other:
;; A Path
;; A Graph (set of nodes & relationships)
;; A traversal (specify a query)

(defprotocol DBObject
  (save [obj])
  (refresh [obj] [obj force?])
  (delete [obj])
  (db-reference [obj]))

(defprotocol Node
  (relations [node] [node direction] [node direction types]))

(defprotocol Relation
  (relation-type [rel])
  (start-node [rel])
  (end-node [rel]))

(defprotocol Path
  (path-head [path])
  (path-tail [path])
  (path-nodes [path])
  (path-relations [path])
  (path-length [path])
  (path-seq [path]))

;;
;; Cached Node and Relationship Objects
;;

(declare as-relation)

(deftype NeoNode [^{:volatile-mutable true} fmap
		  ^{:volatile-mutable true} props]
  
  Object
  (hashCode [this] (clojure.lang.APersistentMap/mapHash this))
  (equals [this gs] (clojure.lang.APersistentMap/mapEquals this gs))
  
  DBObject
  (save [obj]
	(refresh obj nil)
	(put-json (:properties fmap) props)
	obj)
  (refresh [obj] (refresh obj true))
  (refresh [obj force?]
	   (when (or force?
		     (nil? props)
		     (nil? (:all_relationships fmap)))
	     (let [data (get-json (:self fmap))]
	       (set! fmap (dissoc data :data))
	       (set! props (:data data))))
	   obj)
  (delete [obj]
	  (delete-json (:self fmap))
	  nil)
  (db-reference [obj] (:self fmap))

  clojure.lang.IPersistentMap
  (count [obj] (count props))
  (empty [obj] (empty props))
  (cons [obj e] (new NeoNode fmap (cons e props)))
  (entryAt [obj k] (.entryAt props))
  (equiv [obj other]
	 (boolean (or (identical? obj other)
		      (and (= (class obj) (class other))
			   (= (db-reference obj) (db-reference other))
			   (= (seq obj) (seq other))))))
  (containsKey [obj k]
	       (refresh obj nil)
	       (.containsKey props k))
  (seq [obj]
       (refresh obj nil)
       (seq props))
  (assoc [obj k e]
    (refresh obj nil)
    (new NeoNode fmap (assoc props k e)))
  (without [obj k]
	   (refresh obj nil)
	   (new NeoNode fmap (.without props k)))

  clojure.lang.ILookup
  clojure.lang.IKeywordLookup
  (valAt [obj k] (.valAt obj k nil))
  (valAt [obj k else]
	 (refresh obj nil)
	 (.valAt props k else))
  (getLookupThunk [obj k]
		  (reify clojure.lang.ILookupThunk
		    (get [thunk gt]
			 (refresh obj nil)
			 (get props k nil))))

  Node
  (relations [node] (relations node :all nil))
  (relations [node direction] (relations node direction nil))
  (relations [node direction types]
	     (refresh node nil)
	     (let [no-types (empty? types)
		   type-string (string/join "&" types)
		   rel-url 
		   (cond 
		    (and no-types (= :all direction)) (:all_relationships (.fmap node))
		    (= :all direction) (string/replace-re #"\{.*\}" type-string (:all_typed_relationships (.fmap node)))
		    (and no-types (= :in direction)) (:incoming_relationships (.fmap node))
		    (= :in direction) (string/replace-re #"\{.*\}" type-string (:incoming_typed_relationships (.fmap node)))
		    (and no-types (= :out direction)) (:outgoing_relationships (.fmap node))
		    (= :out direction) (string/replace-re #"\{.*}" type-string (:outgoing_typed_relationships (.fmap node))))]
	       (map as-relation (get-json rel-url)))))

(defn as-node [record]
  (cond (map? record)
	(new NeoNode (dissoc record :data) (:data record))
	(string? record)
	(new NeoNode {:self record} nil)
	true (throw (java.lang.Error. "Unrecognized record type"))))

(defn node-id [node]
  (last (string/split #"/" (db-reference node))))

;;
;; Overload print-method for specific nodes based on :type field
;;

(defn print-node-dispatch [node w]
  (:type node))

(defmulti print-node
  "Used to print a node's state in liu of default node method when present"
  :type)

(defmethod print-node :default [node w]
  (.write w "Node")
  (print-method
   (apply hash-map (apply concat (seq node)))
   w))

(defmulti node-short-name
  "Used in relation printing for a short string name for a node of a particular type"
  :type)

(defmethod node-short-name :default [node]
  (node-id node))

(defmethod node-short-name "person" [node]
  (get node :name))

(defmethod print-method NeoNode [n w]
  (print-node n w))

;;
;; Relations
;;

(deftype NeoRelation [^{:volatile-mutable true} type
		      ^{:volatile-mutable true} start
		      ^{:volatile-mutable true} end
		      ^{:volatile-mutable true} props
		      ^{:volatile-mutable true} fmap]
		      
  DBObject
  (save [rel]
	(refresh rel nil)
	(put-json (:properties fmap) props)
	rel)
  (refresh [rel] (refresh rel true))
  (refresh [rel force?]
	   (assert (:self fmap))
	   (when (or force?
		     (nil? props)
		     (nil? start))
	     (let [data (get-json (:self fmap))]
	       (set! fmap (dissoc data :start :end :type :data))
	       (set! props (:data data))
	       (set! type (:type data))
	       (set! start (as-node (:start data)))
	       (set! end (as-node (:end data)))))
	   rel)
  (delete [rel]
	  (delete-json (:self fmap))
	  nil)
  (db-reference [rel]
		(:self fmap))

  Relation
  (relation-type [rel]
		 (refresh rel nil)
		 type)
  (start-node [rel]
	      (refresh rel nil)
	      start)
  (end-node [rel]
	    (refresh rel nil)
	    end)

  clojure.lang.IPersistentMap
  (count [obj] (count props))
  (empty [obj] (empty props))
  (cons [obj e]
	(new NeoRelation type start end (cons e props) fmap))
  (entryAt [obj k] (.entryAt props))
  (equiv [obj other]
	 (refresh obj nil)
	 (boolean (or (identical? obj other)
		      (and (= (class obj) (class other))
			   (= (db-reference obj) (db-reference other))
			   (= (seq obj) (seq other))))))
  (containsKey [obj k]
	       (refresh obj nil)
	       (.containsKey props k))
  (seq [obj]
       (refresh obj nil)
       (seq props))
  (assoc [obj k e]
    (refresh obj nil)
    (new NeoRelation type start end (assoc props k e)  fmap))
  (without [obj k]
	   (refresh obj nil)
    (new NeoRelation type start end (.without props k) fmap))

  clojure.lang.ILookup
  clojure.lang.IKeywordLookup
  (valAt [obj k] (.valAt obj k nil))
  (valAt [obj k else]
	 (refresh obj nil)
	 (.valAt props k else))
  (getLookupThunk [obj k]
		  (reify clojure.lang.ILookupThunk
		    (get [thunk gt]
			 (refresh obj nil)
			 (get props k nil)))))
			   
		  
(defn as-relation [record]
  (cond (map? record)
	(do
	  (assert (:type record))
	  (new NeoRelation
	       (:type record)
	       (as-node (:start record))
	       (as-node (:end record))
	       (or (:data record) {})
	       (dissoc record :type :start :end :data)))
	(string? record)
	(new NeoRelation
	     nil
	     nil
	     nil
	     nil
	     {:self record})
	true (throw java.lang.Error "Unknown record type passed as relation")))

(defmethod print-method NeoRelation [rel w]
  (.write w (str "Rel{'" 
		    (node-short-name (start-node rel))
		    "' -["
		    (relation-type rel)
		    "]-> '"
		    (node-short-name (end-node rel))
		     "'}")))
  

;;
;; Path
;;

(deftype NeoPath [nodes relations]
  Path
  (path-head [path] (first nodes))
  (path-tail [path] (last nodes))
  (path-nodes [path] nodes)
  (path-relations [path] relations)
  (path-length [path] (count relations))
  (path-seq [path] (interleave nodes relations)))

(defn as-path [record]
  (let [{:keys [nodes relationships]} record]
    (NeoPath. (map as-node nodes) (map as-relation relationships))))

(defmethod print-method NeoPath [path w]
  (.write w (str "Path[" (path-length path)
		 " '" (node-short-name (path-head path))
		 "' ... '" (node-short-name (path-tail path))
		 "']")))

