(ns channel-bench
  (:require
    [clojure.core.async :as a]
    [clojure.pprint :as pp]
    [clojure.string :as str]
    [co.multiply.quiescent :as q :refer [qdo qfor]]
    [co.multiply.quiescent.channel :refer [chan pipe poll put! seal! take!]]
    [criterium.core :as c])
  (:import [java.time Duration]
           [java.util.concurrent TimeoutException]))



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
  [{:keys [type n producers consumers buffer make-ch xf quick]}]
  (let [timeout (if quick (Duration/ofMinutes 1) (Duration/ofMinutes 10))]
    (case type
      :ping-pong
      (let [ch-a (make-ch buffer)
            ch-b (make-ch buffer)]
        @(-> (qdo
               (q/task (dotimes [_ n] (put! ch-b (take! ch-a))))
               (q/task (dotimes [_ n] (put! ch-a :ping) (take! ch-b))))
           (q/timeout timeout (TimeoutException. "Benchmark took more than 10 minutes"))))

      :pipe
      (let [ch-a  (if xf (make-ch buffer xf) (make-ch buffer))
            ch-b  (make-ch buffer)
            per-p (quot n producers)]
        @(-> (qdo
               (pipe ch-a ch-b)
               (qfor [_ (range consumers)]
                 (q/task (loop [] (poll [_ ch-b] (recur) nil))))
               (q/task
                 @(qfor [_ (range producers)]
                    (q/task (dotimes [i per-p] (put! ch-a i))))
                 (seal! ch-a)))
           (q/timeout timeout (TimeoutException. "Benchmark took more than 10 minutes"))))

      ;; default: throughput (plain or xf, determined by :xf key)
      (let [ch    (if xf (make-ch buffer xf) (make-ch buffer))
            per-p (quot n producers)]
        @(-> (qdo
               (qfor [_ (range consumers)]
                 (q/task (loop [] (poll [_ ch] (recur) nil))))
               (q/task
                 @(qfor [_ (range producers)]
                    (q/task (dotimes [i per-p] (put! ch i))))
                 (seal! ch)))
           (q/timeout timeout (TimeoutException. "Benchmark took more than 10 minutes")))))))


(defn- run-core-async
  [{:keys [type n producers consumers buffer xf]}]
  (case type
    :ping-pong
    (let [ch-a (a/chan buffer)
          ch-b (a/chan buffer)
          pong (a/go (dotimes [_ n] (a/>! ch-b (a/<! ch-a))))
          ping (a/go (dotimes [_ n] (a/>! ch-a :ping) (a/<! ch-b)))]
      (a/<!! ping)
      (a/<!! pong))

    :pipe
    (let [ch-a (if xf (a/chan buffer xf) (a/chan buffer))
          ch-b (a/chan buffer)
          _    (a/pipe ch-a ch-b)
          cs   (mapv (fn [_] (a/go (loop [] (when-some [_ (a/<! ch-b)] (recur))))) (range consumers))
          ps   (mapv (fn [_] (a/go (dotimes [i (quot n producers)] (a/>! ch-a i)))) (range producers))]
      (run! a/<!! ps)
      (a/close! ch-a)
      (run! a/<!! cs))

    ;; default: throughput (plain or xf, determined by :xf key)
    (let [ch  (if xf (a/chan buffer xf) (a/chan buffer))
          cs  (mapv (fn [_] (a/go (loop [] (when-some [_ (a/<! ch)] (recur))))) (range consumers))
          ps  (mapv (fn [_] (a/go (dotimes [i (quot n producers)] (a/>! ch i)))) (range producers))]
      (run! a/<!! ps)
      (a/close! ch)
      (run! a/<!! cs))))


(defn- run-parallel
  [{:keys [workloads framework] :as cfg}]
  (let [defaults (dissoc cfg :workloads :framework :scenario :group :type)]
    @(qfor [w workloads]
       (qfor [_ (range (:count w))]
         (q/task (run-scenario (merge defaults w {:framework framework})))))))


(defn- run-scenario
  [{:keys [framework] :as cfg}]
  (let [cfg (merge cfg (get frameworks framework))]
    (if (:workloads cfg)
      (run-parallel cfg)
      (case (:model cfg)
        :quiescent (run-quiescent cfg)
        :core-async (run-core-async cfg)))))


;; -- Scenario definitions --

(def scenarios
  [;; ================================================================
   ;; Isolated (single channel, profiling baseline)
   ;; ================================================================
   {:scenario "1P1C" :group :isolated :producers 1 :consumers 1 :n 1000000 :buffer 1024}
   {:scenario "1P1C" :group :isolated :producers 1 :consumers 1 :n 1000000 :buffer 1}
   {:scenario "4P4C" :group :isolated :producers 4 :consumers 4 :n 1000000 :buffer 1024}
   {:scenario "Ping-pong" :group :isolated :type :ping-pong :n 100000 :buffer 1}
   {:scenario  "XF map 1P1C" :group :isolated :xf (map inc)
    :producers 1 :consumers 1 :n 1000000 :buffer 1024}

   ;; ================================================================
   ;; Saturated symmetric (M× parallel, balanced producer/consumer)
   ;; ================================================================
   {:scenario  "50×1P1C" :group :saturated :buffer 64
    :workloads [{:count 50 :producers 1 :consumers 1 :n 100000}]}
   {:scenario  "50×4P4C" :group :saturated :buffer 64
    :workloads [{:count 50 :producers 4 :consumers 4 :n 100000}]}
   {:scenario  "200×1P1C" :group :saturated :buffer 64
    :workloads [{:count 200 :producers 1 :consumers 1 :n 50000}]}
   {:scenario  "200×1P1C buf=1" :group :saturated :buffer 1
    :workloads [{:count 200 :producers 1 :consumers 1 :n 50000}]}
   {:scenario  "Mixed (40 ch)" :group :saturated
    :workloads [{:count 20 :producers 1 :consumers 1 :n 100000 :buffer 64}
                {:count 10 :producers 4 :consumers 4 :n 100000 :buffer 64}
                {:count 10 :type :ping-pong :n 10000 :buffer 1}]}

   ;; ================================================================
   ;; Saturated fan-in (M× NP1C — put-side combining, buf=64)
   ;; n must be divisible by producer count
   ;; ================================================================
   {:scenario  "24×32P1C" :group :fan-in :buffer 64
    :workloads [{:count 24 :producers 32 :consumers 1 :n 96000}]}
   {:scenario  "24×128P1C" :group :fan-in :buffer 64
    :workloads [{:count 24 :producers 128 :consumers 1 :n 96000}]}

   ;; ================================================================
   ;; Saturated fan-out (M× 1PNC — take-side combining, buf=64)
   ;; ================================================================
   {:scenario  "24×1P32C" :group :fan-out :buffer 64
    :workloads [{:count 24 :producers 1 :consumers 32 :n 96000}]}
   {:scenario  "24×1P128C" :group :fan-out :buffer 64
    :workloads [{:count 24 :producers 1 :consumers 128 :n 96000}]}

   ;; ================================================================
   ;; Saturated fan-in XF (put-side combining + transducer, buf=64)
   ;; ================================================================
   {:scenario  "24×XF 32P1C" :group :fan-in-xf :buffer 64
    :workloads [{:count 24 :xf (map identity) :producers 32 :consumers 1 :n 96000}]}
   {:scenario  "24×XF 128P1C" :group :fan-in-xf :buffer 64
    :workloads [{:count 24 :xf (map identity) :producers 128 :consumers 1 :n 96000}]}

   ;; ================================================================
   ;; Saturated fan-out XF (take-side combining + transducer, buf=64)
   ;; ================================================================
   {:scenario  "24×XF 1P32C" :group :fan-out-xf :buffer 64
    :workloads [{:count 24 :xf (map identity) :producers 1 :consumers 32 :n 96000}]}
   {:scenario  "24×XF 1P128C" :group :fan-out-xf :buffer 64
    :workloads [{:count 24 :xf (map identity) :producers 1 :consumers 128 :n 96000}]}

   ;; ================================================================
   ;; Optimal conditions (buf=1024 — does scaling the consumer side
   ;; help when producers saturate the buffer?)
   ;; n must be divisible by producer count
   ;; ================================================================
   {:scenario  "24×128P1C" :group :optimal :buffer 1024
    :workloads [{:count 24 :producers 128 :consumers 1 :n 96000}]}
   {:scenario  "24×128P16C" :group :optimal :buffer 1024
    :workloads [{:count 24 :producers 128 :consumers 16 :n 96000}]}

   ;; ================================================================
   ;; Saturated transducer (XF-specific scenarios under saturation)
   ;; ================================================================
   {:scenario  "50×XF filter 1P1C" :group :saturated-xf :buffer 64
    :workloads [{:count 50 :xf (filter even?) :pass-ratio 0.5
                 :producers 1 :consumers 1 :n 100000}]}
   {:scenario  "50×XF mapcat 1P1C" :group :saturated-xf :buffer 64
    :workloads [{:count 50 :xf (mapcat #(vector % %)) :expand-factor 2
                 :producers 1 :consumers 1 :n 50000}]}
   {:scenario  "50×XF map 4P4C" :group :saturated-xf :buffer 64
    :workloads [{:count 50 :xf (map inc)
                 :producers 4 :consumers 4 :n 100000}]}

   ;; ================================================================
   ;; Saturated pipeline (pipe under saturation)
   ;; ================================================================
   {:scenario  "20×Pipe 4P→4C" :group :saturated-pipe :buffer 64
    :workloads [{:count 20 :type :pipe :producers 4 :consumers 4 :n 100000}]}
   {:scenario  "20×Pipe XF 4P→4C" :group :saturated-pipe :buffer 64
    :workloads [{:count 20 :type :pipe :xf (map inc) :producers 4 :consumers 4 :n 100000}]}])


;; -- Bench harness --

(defn- format-ms
  [v]
  (format "%.3f ms" (* v 1000.0)))


(defn- format-pct
  [v]
  (format "%.1f%%" (* (or v 0) 100.0)))


(defn bench-one
  [cfg framework {:keys [verbose quick]}]
  (let [run-cfg (assoc cfg :framework framework :quick (true? quick))
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
  [& {:keys [only scenario verbose quick frameworks]}]
  (let [active      (cond
                      scenario (filterv #(contains? scenario (:scenario %)) scenarios)
                      only (filterv #(contains? only (:group %)) scenarios)
                      :else scenarios)
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
          (take-while #(not (str/starts-with? % "-")))
          (map #(keyword (str/replace % ":" ""))))
        args))))


(defn- parse-str-list
  "Parse a list of string args after a flag like --scenario."
  [args flag]
  (let [idx (.indexOf args flag)]
    (when (nat-int? idx)
      (into []
        (comp (drop (inc idx))
          (take-while #(not (str/starts-with? % "-"))))
        args))))


(defn -main
  [& args]
  (q/throw-on-platform-park! false)
  (let [args       (vec args)
        verbose    (boolean (some #{"--verbose" "-v"} args))
        quick      (boolean (some #{"--quick" "-q"} args))
        only       (some-> (parse-kw-list args "--only") set)
        scenario   (some-> (parse-str-list args "--scenario") set)
        frameworks (parse-kw-list args "--frameworks")]
    (run-all-benchmarks :only only :scenario scenario :verbose verbose :quick quick
      :frameworks (seq frameworks))))