(ns com.compass.neo4j
  (:use com.compass.neo4j.rest
	com.compass.neo4j.primitives)
  (:require
   [clojure.contrib.string :as string]
   [clj-http.util :as http-util]))

;; A reasonably transparent layer for querying and manipulating Neo graphs
;; over the REST interface.  We can ignore supporting typical Neo graph objects
;; given the JSON encoding and focus on creating a clojure abstraction that is
;; easy to work with and hides the json mechanics.  I focus on minimal optimization
;; for now.  Lazy/cached access is used for ad-hoc queries; specifying transactions
;; forces non-cached operations.
;;
;; Batch operations are supported for uploads.

;;
;; Create and Resolve nodes
;;

(defn get-node [ref]
  (cond (number? ref)
	(as-node (get-json (str (get-in *db* [:root :node]) "/" ref)))
	(string? ref)
	(as-node (get-json ref))))

(defn db-root
  "gets the neo4j database root"
  [db-url]
  (get-node db-url))

(defn reference-node
  "returns the reference node for the db"
  ([] (as-node (:ref *db*)))
  ([dbroot] (as-node (:ref *db*))))

(defn create-node 
  "Creates a node. on success returns the node, on fail returns nil"
  ([properties root]
     (with-db [root]
       (create-node properties)))
  ([properties]
     (assert *db*)
     (let [node-url (get-in *db* [:root :node])]
       (as-node (post-json node-url properties))))
  ([]
     (assert *db*)
     (let [node-url (get-in *db* [:root :node])]
       (as-node (post-json node-url)))))

;;
;; Create and Resolve Relations
;;

(defn create-relation
  "creates a relationshp between two nodes"
  ([start-node end-node rel-type data]
     (let [start-url (db-reference start-node)
	   end-node-url (db-reference end-node)]
       (as-relation
	(post-json (str start-url "/relationships")
		   { "to" end-node-url, "data" data, "type" rel-type}))))
  ([start-node end-node rel-type]
     (create-relation start-node end-node rel-type {})))


;; =======================================================
;; =======================================================

;;
;; Index related functions
;;

(defn create-node-index
  "creates a node index with the specified name and config"
  [dbroot name type provider]
  (let [index-url (dbroot :node_index)
        config { "type" type, "provider" provider }]
    (post-json index-url { "name" name, "config" config })))

(defn node-indices
  "gets existing node indices"
  [dbroot]
  (let [index-url (dbroot :node_index)] 
    (get-json index-url)))

(defn relationship-indices
  "returns existing relationship indices"
  [dbroot]
  (let [index-url (dbroot :relationship_index)]
    (get-json index-url)))

(defn get-node-index
  "gets a node index with the specified name"
  [dbroot name]
  (let [indices (node-indices dbroot)]
    (get indices (keyword name))))

(defn index-type
  "gets the type of an index"
  [index]
  (index :type))

(defn index-provider
  "returns the provider of the index"
  [index]
  (index :provider))

(defn- index-key-val-url
  [index the-key value]
  (let [template-url (index :template)
        key-sub-url (string/replace-re #"\{key\}" (string/as-str the-key) template-url)]
        (string/replace-re #"\{value\}" (http-util/url-encode value) key-sub-url)))

;;
;; Operations on Indexes
;;

(defn add-to-index
  "adds the item to the index"
  [index the-key value obj]
  (let [url (index-key-val-url index the-key value)]
    (post-json url (:self obj))))

(defn index-get
  "does exact match index query"
  [index the-key value]
  (let [url (index-key-val-url index the-key value)]
    (as-node (get-json url))))

(defn delete-from-index
  "deletes a key value pair from index"
  [index obj]
  (try 
    (let [url (obj :indexed)]
      (delete-json url)
      true)
    (catch Exception e false)))


;;
;; Paths
;;

(defn get-path [source target & {:keys [relationships depth algorithm]
				 :or {depth 3 algorithm "shortestPath"}}]
  (as-path
   (post-json (str (db-reference source) "/path")
	      {:to (db-reference target)
	       :relationships relationships
	       "max_depth" depth
	       :algorithm algorithm})))

(defn get-paths [source target & {:keys [relationships depth algorithm]
				 :or {depth 3 algorithm "shortestPath"}}]
  (map as-path
   (post-json (str (db-reference source) "/paths")
	      {:to (db-reference target)
	       :relationships relationships
	       "max_depth" depth
	       :algorithm algorithm})))


(defn traverse [head prune-eval & {:keys [order uniqueness relationships max_depth return]
				   :or {order "depath_first" uniqueness "node_path" max_depth 3}}]
  (assert (string? prune-eval))
  (assert (= (class head) com.ianeslick.neo4j.primitives.NeoNode))
  (map as-path
   (post-json (str (db-reference head) "/traverse/path")
	      {:order order
	       :uniqueness uniqueness
	       :relationships relationships
	       "max_depth" max_depth
	       "prune_evaluator" {:language "javascript"
				  :body prune-eval}
	       "return_filter" {:language "builtin"
				:name "all"}})))
  

