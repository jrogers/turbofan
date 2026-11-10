(ns turbofan.jepsen
  "Jepsen's list-append workload against a cluster of examples/list_append.rs (#14).

  Each node runs `list_append serve`, and beside it `list_append gateway`,
  which Jepsen's clients speak a line of text to: the gateway holds a session
  with the cluster for each connection, resent through failovers as any
  client's is. A request is `a <k> <v>;r <k>`, and its answer `ok <EDN txn>`,
  `fail <why>` (never applied) or `info <why>` (maybe applied)."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.lazyfs :as lazyfs]
            [jepsen.nemesis :as n]
            [jepsen.nemesis.combined :as nemesis]
            [jepsen.os.debian :as debian]
            [jepsen.tests.cycle.append :as append])
  (:import (java.io BufferedReader InputStreamReader OutputStreamWriter)
           (java.net InetSocketAddress Socket)
           (java.nio.charset StandardCharsets)))

(def dir "/opt/turbofan")
(def binary (str dir "/list_append"))
(def data-dir (str dir "/data"))
(def node-log (str dir "/node.log"))
(def gateway-log (str dir "/gateway.log"))
(def node-pidfile (str dir "/node.pid"))
(def gateway-pidfile (str dir "/gateway.pid"))
;; A node removed from the cluster, and its data with it: never started again
;; until it's added back.
(def removed-marker (str dir "/removed"))
;; A node added back: it starts with --join from then on, its members its
;; log's, not the command line's.
(def join-marker (str dir "/join"))

(def peer-port 7001)
(def client-port 7101)
(def admin-port 7201)
(def gateway-port 7301)

(def admin-token "jepsen-admin")
(def peer-token "jepsen-peer")

;; How long the gateway waits for an answer, and Jepsen's client for the
;; gateway's: longer, so the gateway's `info timeout` comes first.
(def gateway-timeout-ms 5000)
(def socket-timeout-ms 10000)

(defn node-id
  "Node n3's ID, 3: by its place in the test's nodes."
  [test node]
  (inc (.indexOf ^java.util.List (:nodes test) node)))

(defn members
  "`1=n1:7001,2=n2:7001,...` for `port`."
  [test port]
  (->> (:nodes test)
       (map (fn [node] (str (node-id test node) "=" node ":" port)))
       (str/join ",")))

(def env {:TURBOFAN_ADMIN_TOKEN admin-token, :TURBOFAN_PEER_TOKEN peer-token})

(defn start-node! [test node]
  (apply cu/start-daemon!
         {:logfile node-log, :pidfile node-pidfile, :chdir dir, :env env}
         binary
         (cond-> [:serve
                  :--id      (node-id test node)
                  :--data    data-dir
                  :--listen  "0.0.0.0"
                  :--peers   (members test peer-port)
                  :--clients (members test client-port)
                  :--admin   (members test admin-port)]
           (not (:fsync test))     (conj :--no-fsync)
           (cu/exists? join-marker) (conj :--join))))

(defn start-gateway! [test node]
  (cu/start-daemon!
    {:logfile gateway-log, :pidfile gateway-pidfile, :chdir dir}
    binary
    :gateway
    :--listen     (str "0.0.0.0:" gateway-port)
    :--clients    (->> (:nodes test) (map #(str % ":" client-port)) (str/join ","))
    :--timeout-ms gateway-timeout-ms))

(defn signal!
  "Sends `signal` to the process in `pidfile`, if there's one: the node and the
  gateway run the same binary, so killing by name would take both."
  [signal pidfile]
  (c/exec :sh :-c (str "test -f " pidfile " && kill -" signal " $(cat " pidfile ") || true")))

(defn db
  "The cluster, its binary copied from the control node's `(:binary test)`.
  With `:lazyfs`, its data directory is on LazyFS, and a node killed loses what
  it wrote but didn't sync, as one that lost power does."
  [opts]
  (let [lazyfs (when (:lazyfs opts) (lazyfs/db data-dir))]
    (reify
      db/DB
      (setup! [_ test node]
        (c/su
          (c/exec :mkdir :-p dir)
          (c/upload (:binary test) binary)
          (c/exec :chmod :+x binary)
          (when lazyfs (db/setup! lazyfs test node))
          (start-node! test node)
          (start-gateway! test node)))

      (teardown! [_ test node]
        (c/su
          (signal! "KILL" gateway-pidfile)
          (signal! "KILL" node-pidfile)
          (when lazyfs (db/teardown! lazyfs test node))
          (c/exec :rm :-rf dir)))

      db/Kill
      (start! [_ test node]
        (c/su (when-not (cu/exists? removed-marker) (start-node! test node))))
      (kill! [_ test node]
        (c/su
          (signal! "KILL" node-pidfile)
          (c/exec :rm :-f node-pidfile)
          (when lazyfs (lazyfs/lose-unfsynced-writes! lazyfs))))

      db/Pause
      (pause! [_ test node] (c/su (signal! "STOP" node-pidfile)))
      (resume! [_ test node] (c/su (signal! "CONT" node-pidfile)))

      db/Primary
      (setup-primary! [_ test node])
      (primaries [_ test]
        ;; The leader, as any node that answers says it is.
        (or (some (fn [node]
                    (try
                      (c/on node
                        (let [out (c/exec :timeout 2 :env (str "TURBOFAN_ADMIN_TOKEN=" admin-token)
                                          binary :status :--admin (str node ":" admin-port))
                              id  (some->> out (re-find #"leader (\d+)") second parse-long)]
                          (when (and id (<= 1 id (count (:nodes test))))
                            [(nth (:nodes test) (dec id))])))
                      (catch Exception _ nil)))
                  (:nodes test))
            []))

      db/LogFiles
      (log-files [_ test node]
        (cond-> [node-log gateway-log] lazyfs (conj (:log-file (:lazyfs lazyfs))))))))

(defn encode
  "A transaction's request line: `a 1 5;r 1`."
  [txn]
  (->> txn
       (map (fn [[f k v]]
              (case f
                :append (str "a " k " " v)
                :r      (str "r " k))))
       (str/join ";")))

(defn read-only? [txn]
  (every? (comp #{:r} first) txn))

(defn answer
  "Completes `op` with the gateway's `line`."
  [op line]
  (let [[kind rest] (str/split (or line "info connection-closed") #" " 2)]
    (case kind
      "ok"   (assoc op :type :ok, :value (edn/read-string rest))
      "fail" (assoc op :type :fail, :error (keyword rest))
      "info" (assoc op :type (if (read-only? (:value op)) :fail :info), :error (keyword rest)))))

(defrecord Client [^Socket socket ^BufferedReader in ^OutputStreamWriter out]
  client/Client
  (open! [this test node]
    (let [socket (doto (Socket.)
                   (.connect (InetSocketAddress. ^String node (int gateway-port)) 2000)
                   (.setSoTimeout (int socket-timeout-ms))
                   (.setTcpNoDelay true))]
      (assoc this
             :socket socket
             :in     (BufferedReader. (InputStreamReader. (.getInputStream socket) StandardCharsets/UTF_8))
             :out    (OutputStreamWriter. (.getOutputStream socket) StandardCharsets/UTF_8))))

  (setup! [_ _])

  (invoke! [_ _ op]
    (try
      (.write out (str (encode (:value op)) "\n"))
      (.flush out)
      (answer op (.readLine in))
      (catch java.io.IOException e
        ;; Sent, perhaps, and no answer: a read logs nothing, so it failed.
        (assoc op :type (if (read-only? (:value op)) :fail :info) :error [:io (.getMessage e)]))))

  (teardown! [_ _])

  (close! [_ _]
    (when socket (.close socket))))

(defn admin!
  "Runs operator `command` on node `via`, against its own admin listener,
  returning what it prints; passed on to the leader. Given up after `secs`: a
  node cut off from the leader waits for it, so try another."
  [secs via command & args]
  (c/on via
    (apply c/exec :timeout secs :env (str "TURBOFAN_ADMIN_TOKEN=" admin-token)
           binary command :--admin (str via ":" admin-port) args)))

(defn admin-any!
  "As admin!, through each of `vias` in turn until one answers: what it
  printed, or the last error."
  [secs vias command & args]
  (loop [[via & more] (shuffle vias)]
    (let [result (try [:ok (apply admin! secs via command args)]
                      (catch Exception e [:error (.getMessage e)]))]
      (if (or (= :ok (first result)) (empty? more))
        result
        (recur more)))))

(defn cluster-members
  "The members, as the first node to answer `status` says, or nil if none does."
  [test]
  (some (fn [via]
          (try
            (let [ids (->> (admin! 2 via :status) (re-seq #"(?m)^member (\d+)") (map (comp parse-long second)))]
              (when (seq ids) (set (map #(nth (:nodes test) (dec %)) ids))))
            (catch Exception _ nil)))
        (shuffle (:nodes test))))

(defn stop-and-wipe!
  "Stops the node on this machine and empties its data directory (on LazyFS,
  the mount itself stays)."
  []
  (signal! "KILL" node-pidfile)
  (c/exec :rm :-f node-pidfile)
  (c/exec :mkdir :-p data-dir)
  (c/exec :find data-dir :-mindepth 1 :-delete))

(defn remove-member!
  "Removes `node`, then, once `status` says it's gone, stops it and drops its
  data. A removal that timed out may have happened, or happen later: status
  says which."
  [test members node]
  (let [result (admin-any! 10 (disj members node) :remove-member :--id (node-id test node))
        now    (cluster-members test)]
    (if (and now (not (now node)))
      (do (c/on node (c/su (c/exec :touch removed-marker) (stop-and-wipe!)))
          [:removed node now])
      [:not-removed node (second result)])))

(defn add-member!
  "Starts `node` empty, joining, and asks the cluster to add it, as a node
  replaced is."
  [test members node]
  (let [spec (str (node-id test node) "=" node ":" peer-port "," node ":" client-port "," node ":" admin-port)]
    (c/on node
      (c/su (stop-and-wipe!)
            (c/exec :rm :-f removed-marker)
            (c/exec :touch join-marker)
            (start-node! test node)))
    ;; Caught up, then added: a moment, for a log this short.
    (let [[outcome detail] (admin-any! 30 members :add-member :--member spec)]
      (if (= :ok outcome)
        [:added node (cluster-members test)]
        [:not-added node detail]))))

(defn member-nemesis
  "Removes a member, or adds back one removed, one at a time: never fewer than
  three voters."
  []
  (reify
    n/Reflection
    (fs [_] #{:member})

    n/Nemesis
    (setup! [this _] this)
    (invoke! [_ test op]
      (let [members (cluster-members test)
            out     (when members (remove members (:nodes test)))]
        (assoc op :value
               (cond
                 (nil? members)
                 [:no-status]

                 (and (seq out) (or (<= (count members) 3) (< (rand) 0.5)))
                 (add-member! test members (rand-nth (vec out)))

                 (> (count members) 3)
                 (remove-member! test members (rand-nth (vec members)))

                 :else
                 [:nothing members]))))
    (teardown! [_ _])))

(defn member-package
  "Membership changes, as a package for jepsen.nemesis.combined."
  [opts]
  (when ((:nemesis opts) :member)
    {:generator (->> (gen/repeat {:type :info, :f :member})
                     (gen/stagger (:nemesis-interval opts)))
     :nemesis   (member-nemesis)
     :perf      #{{:name "member", :fs #{:member}, :color "#E9A0E6"}}}))

(defn turbofan-test
  "The test, from the command line's options."
  [opts]
  (let [db       (db opts)
        workload (append/test {:key-count          10
                               :min-txn-length     1
                               :max-txn-length     4
                               :max-writes-per-key 128
                               :consistency-models [:strict-serializable]})
        package  (nemesis/nemesis-packages
                   {:db        db
                    :nodes     (:nodes opts)
                    :faults    (:nemesis opts)
                    :partition {:targets [:one :primaries :majority :majorities-ring]}
                    :pause     {:targets [:one :primaries :minority]}
                    :kill      {:targets [:one :primaries :minority :all]}
                    ;; Jepsen's default is no disruption at all.
                    :packet    {:targets   [:one :primaries :minority :majority :all]
                                :behaviors [{:delay {}}
                                            {:loss {}}
                                            {:delay {}, :reorder {}}
                                            {:duplicate {}}
                                            {:corrupt {}}
                                            {:rate {}}
                                            {:delay {}, :loss {}, :duplicate {}}]}
                    :interval  (:nemesis-interval opts)})
        package  (nemesis/compose-packages (keep identity (conj package (member-package opts))))]
    (merge tests/noop-test
           opts
           {:name            (->> [(str/join "," (map name (sort (:nemesis opts))))
                                       (if (:fsync opts) "fsync" "no-fsync")
                                       (when (:lazyfs opts) "lazyfs")]
                                      (remove str/blank?)
                                      (str/join " ")
                                      (str "turbofan list-append "))
            :os              debian/os
            :db              db
            :client          (map->Client {})
            :nemesis         (:nemesis package)
            :pure-generators true
            :checker         (checker/compose
                               {:perf     (checker/perf {:nemeses (:perf package)})
                                :stats    (checker/stats)
                                :timeline (timeline/html)
                                :crash    (checker/log-file-pattern #"panicked at" "node.log")
                                :workload (:checker workload)})
            :generator       (gen/phases
                               (->> (:generator workload)
                                    (gen/stagger (/ (:rate opts)))
                                    (gen/nemesis (gen/phases (gen/sleep 5) (:generator package)))
                                    (gen/time-limit (:time-limit opts)))
                               (gen/log "Healing")
                               (gen/nemesis (:final-generator package))
                               (gen/log "Waiting for recovery")
                               (gen/sleep 10)
                               (gen/clients (:final-generator workload)))})))

(def faults #{:partition :kill :pause :clock :packet :member})

(def cli-opts
  [[nil "--binary PATH" "examples/list_append, built for the nodes' platform, on the control node."
    :default "/turbofan/target/jepsen/release/examples/list_append"]
   [nil "--fsync" "Run the nodes as by default, syncing the log. Without it, --no-fsync."
    :default false]
   [nil "--lazyfs" "The nodes' data on LazyFS: a node killed loses what it didn't sync."
    :default false]
   [nil "--nemesis FAULTS" "Faults to inject, comma-separated: partition, kill, pause, clock, packet, member; or none."
    :default #{}
    :parse-fn (fn [s] (if (= s "none") #{} (set (map keyword (str/split s #",")))))
    :validate [#(every? faults %) (str "Faults are some of " (str/join ", " (map name faults)))]]
   [nil "--nemesis-interval SECS" "Seconds between faults."
    :default 10
    :parse-fn parse-double]
   [nil "--rate HZ" "Transactions a second, all clients together."
    :default 100
    :parse-fn parse-double]])

(defn -main [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn turbofan-test, :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
