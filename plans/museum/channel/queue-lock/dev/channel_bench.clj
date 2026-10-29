(ns channel-bench
  (:require
    [clojure.core.async :as a]
    [clojure.pprint :as pp]
    [clojure.string :as str]
    [co.multiply.quiescent :as q :refer [qdo qfor]]
    [co.multiply.quiescent.channel :refer [chan pipe poll put! seal! take!]]
    [criterium.core :as c]))



;; -- Channel factories --
;;
;; A framework is: a channel constructor (1-arity plain, 2-arity with xf),
;; an execution model (:quiescent or :core-async), and a display name.
;; Adding a new channel implementation = adding one map entry.

(def frameworks
  {:quiescent  {:model :quiescent :make-ch chan :name "Quiescent"}
   :core-async {:model :core-async :name "core.async"}})


;; -- Runners --
;;
;; Topology (throughput, ping-pong, pipe) is orthogonal to channel
;; construction. The quiescent runner covers all quiescent variants
;; through the factory. core.async is a genuinely different execution
;; model and gets its own runner.

(declare run-scenario)

(defn- run-quiescent
  [{:keys [type n producers consumers buffer make-ch xf]}]
  (case (or type :throughput)
    (:throughput :xform :xform-mapcat :xform-filter)
    (let [ch    (if xf (make-ch buffer xf) (make-ch buffer))
          per-p (quot n producers)]
      @(qdo
         (qfor [_ (range consumers)]
           (q/task (loop [] (poll [_ ch] (recur) nil))))
         (q/task
           @(qfor [_ (range producers)]
              (q/task (dotimes [i per-p] (put! ch i))))
           (seal! ch))))

    :ping-pong
    (let [ch-a (make-ch buffer)
          ch-b (make-ch buffer)]
      @(qdo
         (q/task (dotimes [_ n] (put! ch-b (take! ch-a))))
         (q/task (dotimes [_ n] (put! ch-a :ping) (take! ch-b)))))

    (:pipe :pipe-xf)
    (let [ch-a  (if xf (make-ch buffer xf) (make-ch buffer))
          ch-b  (make-ch buffer)
          per-p (quot n producers)]
      @(qdo
         (pipe ch-a ch-b)
         (qfor [_ (range consumers)]
           (q/task (loop [] (poll [_ ch-b] (recur) nil))))
         (q/task
           @(qfor [_ (range producers)]
              (q/task (dotimes [i per-p] (put! ch-a i))))
           (seal! ch-a))))))


(defn- run-core-async
  [{:keys [type n producers consumers buffer xf expand-factor pass-ratio]}]
  (case (or type :throughput)
    (:throughput :xform :xform-mapcat :xform-filter)
    (let [ch     (if xf (a/chan buffer xf) (a/chan buffer))
          take-n (quot (long (* n (or expand-factor 1) (or pass-ratio 1))) consumers)
          gos    (into
                   (mapv (fn [_] (a/go (dotimes [_ take-n] (a/<! ch)))) (range consumers))
                   (mapv (fn [_] (a/go (dotimes [i (quot n producers)] (a/>! ch i)))) (range producers)))]
      (run! a/<!! gos))

    :ping-pong
    (let [ch-a (a/chan buffer)
          ch-b (a/chan buffer)
          pong (a/go (dotimes [_ n] (a/>! ch-b (a/<! ch-a))))
          ping (a/go (dotimes [_ n] (a/>! ch-a :ping) (a/<! ch-b)))]
      (a/<!! ping)
      (a/<!! pong))

    (:pipe :pipe-xf)
    (let [ch-a (if xf (a/chan buffer xf) (a/chan buffer))
          ch-b (a/chan buffer)
          _    (a/pipe ch-a ch-b)
          cs   (mapv (fn [_] (a/go (dotimes [_ (quot n consumers)] (a/<! ch-b)))) (range consumers))
          ps   (mapv (fn [_] (a/go (dotimes [i (quot n producers)] (a/>! ch-a i)))) (range producers))]
      (run! a/<!! ps)
      (a/close! ch-a)
      (run! a/<!! cs))))


(defn- run-parallel
  [{:keys [workloads framework]}]
  @(qfor [w workloads]
     (qfor [_ (range (:count w))]
       (q/task (run-scenario (assoc w :framework framework))))))


(defn- run-scenario
  [{:keys [framework] :as cfg}]
  (let [cfg (merge cfg (get frameworks framework))]
    (if (= (:type cfg) :parallel)
      (run-parallel cfg)
      (case (:model cfg)
        :quiescent (run-quiescent cfg)
        :core-async (run-core-async cfg)))))


;; -- Scenario definitions --

(def scenarios
  [;; --- Isolated (single channel, buf=1024) ---
   {:scenario "1P1C" :group :isolated :producers 1 :consumers 1 :n 1000000 :buffer 1024}
   {:scenario "1P4C" :group :isolated :producers 1 :consumers 4 :n 1000000 :buffer 1024}
   {:scenario "4P1C" :group :isolated :producers 4 :consumers 1 :n 1000000 :buffer 1024}
   {:scenario "4P4C" :group :isolated :producers 4 :consumers 4 :n 1000000 :buffer 1024}
   {:scenario "Ping-pong" :group :isolated :type :ping-pong :n 100000 :buffer 1}

   ;; --- Small buffer (parking-heavy) ---
   {:scenario "1P1C" :group :small-buffer :producers 1 :consumers 1 :n 1000000 :buffer 1}
   {:scenario "1P1C" :group :small-buffer :producers 1 :consumers 1 :n 1000000 :buffer 16}
   {:scenario "4P4C" :group :small-buffer :producers 4 :consumers 4 :n 1000000 :buffer 1}
   {:scenario "4P4C" :group :small-buffer :producers 4 :consumers 4 :n 1000000 :buffer 16}

   ;; --- System (parallel contention) ---
   {:scenario  "50×1P1C" :group :system :type :parallel
    :workloads [{:count 50 :producers 1 :consumers 1 :n 100000 :buffer 64}]}
   {:scenario  "50×4P4C" :group :system :type :parallel
    :workloads [{:count 50 :producers 4 :consumers 4 :n 100000 :buffer 64}]}
   {:scenario  "Mixed (40 ch)" :group :system :type :parallel
    :workloads [{:count 20 :producers 1 :consumers 1 :n 100000 :buffer 64}
                {:count 10 :producers 4 :consumers 4 :n 100000 :buffer 64}
                {:count 10 :type :ping-pong :n 10000 :buffer 1}]}
   {:scenario  "200×1P1C" :group :system :type :parallel
    :workloads [{:count 200 :producers 1 :consumers 1 :n 50000 :buffer 64}]}

   ;; --- Transducer ---
   {:scenario  "XF map 1P1C" :group :transducer :type :xform :xf (map inc)
    :producers 1 :consumers 1 :n 1000000 :buffer 1024}
   {:scenario  "XF map 4P4C" :group :transducer :type :xform :xf (map inc)
    :producers 4 :consumers 4 :n 1000000 :buffer 1024}
   {:scenario  "XF filter 1P1C" :group :transducer :type :xform-filter :xf (filter even?) :pass-ratio 0.5
    :producers 1 :consumers 1 :n 1000000 :buffer 1024}
   {:scenario  "XF mapcat 1P1C" :group :transducer :type :xform-mapcat :xf (mapcat #(vector % %)) :expand-factor 2
    :producers 1 :consumers 1 :n 500000 :buffer 1024}

   ;; --- Pipeline (pipe) ---
   {:scenario  "Pipe 4P→1P→4C" :group :pipe :type :pipe
    :producers 4 :consumers 4 :n 1000000 :buffer 16}
   {:scenario  "Pipe XF 4P→1P→4C" :group :pipe :type :pipe-xf :xf (map inc)
    :producers 4 :consumers 4 :n 1000000 :buffer 16}
   {:scenario  "Pipe 4P→1P→4C" :group :pipe :type :pipe
    :producers 4 :consumers 4 :n 1000000 :buffer 64}
   {:scenario  "Pipe XF 4P→1P→4C" :group :pipe :type :pipe-xf :xf (map inc)
    :producers 4 :consumers 4 :n 1000000 :buffer 64}
   {:scenario  "Pipe 4P→1P→4C" :group :pipe :type :pipe
    :producers 4 :consumers 4 :n 1000000 :buffer 1024}
   {:scenario  "Pipe XF 4P→1P→4C" :group :pipe :type :pipe-xf :xf (map inc)
    :producers 4 :consumers 4 :n 1000000 :buffer 1024}

   ;; --- Pipeline (parallel contention) ---
   {:scenario  "20×Pipe 4P→1P→4C" :group :pipe :type :parallel
    :workloads [{:count 20 :type :pipe :producers 4 :consumers 4 :n 100000 :buffer 64}]}
   {:scenario  "20×Pipe 4P→1P→4C" :group :pipe :type :parallel
    :workloads [{:count 20 :type :pipe :producers 4 :consumers 4 :n 100000 :buffer 1024}]}

   ;; --- Fan-in (many producers, 1 consumer) ---
   ;; n must be divisible by producer count to avoid deadlock from truncation
   {:scenario "16P1C" :group :fan-in :producers 16 :consumers 1 :n 960000 :buffer 64}
   {:scenario "32P1C" :group :fan-in :producers 32 :consumers 1 :n 960000 :buffer 64}
   {:scenario "64P1C" :group :fan-in :producers 64 :consumers 1 :n 960000 :buffer 64}
   {:scenario "128P1C" :group :fan-in :producers 128 :consumers 1 :n 960000 :buffer 64}
   {:scenario "16P1C" :group :fan-in :producers 16 :consumers 1 :n 960000 :buffer 1024}
   {:scenario "32P1C" :group :fan-in :producers 32 :consumers 1 :n 960000 :buffer 1024}
   {:scenario "64P1C" :group :fan-in :producers 64 :consumers 1 :n 960000 :buffer 1024}
   {:scenario "128P1C" :group :fan-in :producers 128 :consumers 1 :n 960000 :buffer 1024}

   ;; --- Fan-in XF (lock effect on producer contention) ---
   {:scenario  "XF 16P1C" :group :fan-in-xf :type :xform :xf (map identity)
    :producers 16 :consumers 1 :n 960000 :buffer 64}
   {:scenario  "XF 32P1C" :group :fan-in-xf :type :xform :xf (map identity)
    :producers 32 :consumers 1 :n 960000 :buffer 64}
   {:scenario  "XF 64P1C" :group :fan-in-xf :type :xform :xf (map identity)
    :producers 64 :consumers 1 :n 960000 :buffer 64}
   {:scenario  "XF 128P1C" :group :fan-in-xf :type :xform :xf (map identity)
    :producers 128 :consumers 1 :n 960000 :buffer 64}
   {:scenario  "XF 16P1C" :group :fan-in-xf :type :xform :xf (map identity)
    :producers 16 :consumers 1 :n 960000 :buffer 1024}
   {:scenario  "XF 32P1C" :group :fan-in-xf :type :xform :xf (map identity)
    :producers 32 :consumers 1 :n 960000 :buffer 1024}
   {:scenario  "XF 64P1C" :group :fan-in-xf :type :xform :xf (map identity)
    :producers 64 :consumers 1 :n 960000 :buffer 1024}
   {:scenario  "XF 128P1C" :group :fan-in-xf :type :xform :xf (map identity)
    :producers 128 :consumers 1 :n 960000 :buffer 1024}])


;; -- Bench harness --

(defn- format-ms
  [v]
  (format "%.3f ms" (* v 1000.0)))


(defn- format-pct
  [v]
  (format "%.1f%%" (* (or v 0) 100.0)))


(defn bench-one
  [cfg framework {:keys [verbose quick]}]
  (let [run-cfg (assoc cfg :framework framework)
        label   (:scenario cfg)
        ch-name (:name (get frameworks framework))]
    (println (str "\nRunning: " label " — " ch-name))
    (let [res         (if quick
                        (if verbose
                          (c/with-progress-reporting (c/quick-benchmark (run-scenario run-cfg) {}))
                          (c/quick-benchmark (run-scenario run-cfg) {}))
                        (if verbose
                          (c/with-progress-reporting (c/benchmark (run-scenario run-cfg) {}))
                          (c/benchmark (run-scenario run-cfg) {})))
          mean        (first (:mean res))
          variance    (first (:variance res))
          std-dev     (Math/sqrt (double variance))
          lower-q     (first (:lower-q res))
          upper-q     (first (:upper-q res))
          outlier-var (:outlier-variance res)]
      (merge cfg
        {:framework   framework
         :label       label
         :channel     ch-name
         :raw-mean    mean
         :mean        (format-ms mean)
         :std-dev     (format-ms std-dev)
         :lower-q     (format-ms lower-q)
         :upper-q     (format-ms upper-q)
         :outlier-var (format-pct outlier-var)}))))


(def ^:private cols [:label :buffer :channel :mean :std-dev :lower-q :upper-q :outlier-var :speedup])


(defn- row-str
  [r]
  (str "|" (str/join "|" (map #(get r %) cols)) "|"))


(defn- add-speedup
  [cohort]
  (let [max-mean (transduce (map :raw-mean) max 0.0 cohort)]
    (mapv (fn [r]
            (let [ratio (/ max-mean (:raw-mean r))]
              (assoc r :speedup (format "%.1fx" ratio))))
      cohort)))


(defn- emit-header
  [out-file]
  (spit out-file
    (str "## Benchmark Results\n\n"
      "|" (str/join "|" (map name cols)) "|\n"
      "|" (str/join "|" (repeat (count cols) "---")) "|\n")))


(defn- emit-rows
  [out-file cohort]
  (doseq [r cohort]
    (spit out-file (str (row-str r) "\n") :append true)))


(defn run-all-benchmarks
  [& {:keys [only verbose quick frameworks]}]
  (let [active      (if only
                      (filterv #(contains? only (:group %)) scenarios)
                      scenarios)
        default-fws [:quiescent :core-async]
        opts        {:verbose verbose :quick quick}
        out-file    "benchmark_results.md"]

    (emit-header out-file)

    (let [results (into []
                    (mapcat (fn [cfg]
                              (let [fws    (or frameworks
                                             (:frameworks cfg)
                                             default-fws)
                                    cohort (-> (mapv #(bench-one cfg % opts) fws)
                                             add-speedup)]
                                (emit-rows out-file cohort)
                                cohort)))
                    active)]

      (println "\n\n=== BENCHMARK RESULTS ===")
      (pp/print-table cols results)
      results)))


(defn- parse-kw-list
  "Parse a list of keyword args after a flag like --only or --frameworks."
  [args flag]
  (let [idx (.indexOf args flag)]
    (when (nat-int? idx)
      (into []
        (comp (drop (inc idx))
          (take-while #(not (str/starts-with? % "--")))
          (map #(keyword (str/replace % ":" ""))))
        args))))


(defn -main
  [& args]
  (q/throw-on-platform-park! false)
  (let [args       (vec args)
        verbose    (boolean (some #{"--verbose" "-v"} args))
        quick      (boolean (some #{"--quick" "-q"} args))
        only       (some-> (parse-kw-list args "--only") set)
        frameworks (parse-kw-list args "--frameworks")]
    (run-all-benchmarks :only only :verbose verbose :quick quick
      :frameworks (seq frameworks))))