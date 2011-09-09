(ns com.compass.neo4j.rest
  (:require
   [clj-http.client :as client]
   [clj-http.util :as http-util]
   [clojure.contrib.json :as json]
   [clojure.contrib.string :as string]))

;;
;; IO Layer
;;

(defn get-json [url]
  (let [response (client/get url {:accept :json})
	body (:body response)]
    (if (nil? body)
      nil
      (json/read-json body))))

(defn post-json 
  "Will post the data as json.
   Will return json response data or nil."
  [url data]
  (try 
    (let [response (client/post url {:accept :json :content-type :json :body (json/json-str data)})]
      (json/read-json (response :body)))
    (catch Exception e nil)))

(defn put-json
  [url data]
  (try
    (let [response (client/put url {:accept :json :content-type :json :body (json/json-str data)})
          body (response :body)]
      (if (nil? body) true (json/read-json body)))
    (catch Exception e nil)))


(defn delete-json
  [url]
  (try
    (do (client/delete url)
	true)
    (catch Exception e false)))

;;
;; Database functions
;;

(defonce *db* nil)

(defn as-db [base]
  (cond (string? base)
	(let [root (get-json base)]
	  {:url base :root root :ref (get-json (:reference_node root))})
	(map? base) base
	true (throw (java.lang.Error. (format "Unrecognized db reference type %s" ref)))))
    
(defn set-database! [url]
  (alter-var-root #'*db* #(as-db %2) url)
  true)

(defn set-local-database! [& path]
  (set-database! (str "http://localhost:7474/" (or path "db/data"))))

(defn get-ref-cmd [key]
  (assert *db*)
  ((:ref *db*) key))

(defmacro with-db [[db] & body]
  `(binding [*db* (as-db ~db)]
     ~@body))
