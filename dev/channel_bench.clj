(ns channel-bench
  (:require
    [clojure.core.async :as a]
    [clojure.pprint :as pp]
    [clojure.string :as str]
    [co.multiply.quiescent :as q :refer [qdo qfor]]
    [co.multiply.quiescent.channel :refer [chan pipe put! seal! take!]]
    [criterium.core :as c])
  (:import
    [co.multiply.quiescent.impl.channel BoundedChannelAdaptive BoundedChannelLocked]))


;; -- Runners: dispatch on [:type :framework] --

(defmulti run-scenario (fn [cfg] [(:type cfg :throughput) (:framework cfg)]))


(defmethod run-scenario [:throughput :quiescent]
  [{:keys [n producers consumers buffer]}]
  (let [ch    (chan buffer)
        per-p (quot n producers)
        per-c (quot n consumers)]
    @(qdo
       (qfor [_ (range consumers)]
         (q/task
           (dotimes [_ per-c] (take! ch))))
       (qfor [_ (range producers)]
         (q/task
           (dotimes [i per-p] (put! ch i)))))))


(defmethod run-scenario [:throughput :quiescent-locked]
  [{:keys [n producers consumers buffer]}]
  (let [ch    (BoundedChannelLocked. (int buffer))
        per-p (quot n producers)
        per-c (quot n consumers)]
    @(qdo
       (qfor [_ (range consumers)]
         (q/task
           (dotimes [_ per-c] (take! ch))))
       (qfor [_ (range producers)]
         (q/task
           (dotimes [i per-p] (put! ch i)))))))


(defmethod run-scenario [:throughput :quiescent-adaptive]
  [{:keys [n producers consumers buffer]}]
  (let [ch    (BoundedChannelAdaptive. (int buffer))
        per-p (quot n producers)
        per-c (quot n consumers)]
    @(qdo
       (qfor [_ (range consumers)]
         (q/task
           (dotimes [_ per-c] (take! ch))))
       (qfor [_ (range producers)]
         (q/task
           (dotimes [i per-p] (put! ch i)))))))


(defmethod run-scenario [:throughput :core-async]
  [{:keys [n producers consumers buffer]}]
  (let [ch  (a/chan buffer)
        gos (into
              (mapv (fn [_] (a/go (dotimes [_ (quot n consumers)] (a/<! ch)))) (range consumers))
              (mapv (fn [_] (a/go (dotimes [i (quot n producers)] (a/>! ch i)))) (range producers)))]
    (run! a/<!! gos)))


(defmethod run-scenario [:ping-pong :quiescent]
  [{:keys [n buffer]}]
  (let [ch-a (chan buffer)
        ch-b (chan buffer)]
    @(qdo
       (q/task (dotimes [_ n] (put! ch-b (take! ch-a))))
       (q/task (dotimes [_ n] (put! ch-a :ping) (take! ch-b))))))


(defmethod run-scenario [:ping-pong :quiescent-locked]
  [{:keys [n buffer]}]
  (let [ch-a (BoundedChannelLocked. (int buffer))
        ch-b (BoundedChannelLocked. (int buffer))]
    @(qdo
       (q/task (dotimes [_ n] (put! ch-b (take! ch-a))))
       (q/task (dotimes [_ n] (put! ch-a :ping) (take! ch-b))))))


(defmethod run-scenario [:ping-pong :quiescent-adaptive]
  [{:keys [n buffer]}]
  (let [ch-a (BoundedChannelAdaptive. (int buffer))
        ch-b (BoundedChannelAdaptive. (int buffer))]
    @(qdo
       (q/task (dotimes [_ n] (put! ch-b (take! ch-a))))
       (q/task (dotimes [_ n] (put! ch-a :ping) (take! ch-b))))))


(defmethod run-scenario [:ping-pong :core-async]
  [{:keys [n buffer]}]
  (let [ch-a (a/chan buffer)
        ch-b (a/chan buffer)
        pong (a/go (dotimes [_ n] (a/>! ch-b (a/<! ch-a))))
        ping (a/go (dotimes [_ n] (a/>! ch-a :ping) (a/<! ch-b)))]
    (a/<!! ping)
    (a/<!! pong)))


(defmethod run-scenario [:parallel :quiescent]
  [{:keys [workloads]}]
  (let [fns (into []
              (mapcat (fn [{:keys [count] :as w}]
                        (let [cfg (assoc w :framework :quiescent)]
                          (repeat count #(run-scenario cfg)))))
              workloads)]
    @(qfor [f fns] (q/task (f)))))


(defmethod run-scenario [:parallel :quiescent-locked]
  [{:keys [workloads]}]
  (let [fns (into []
              (mapcat (fn [{:keys [count] :as w}]
                        (let [cfg (assoc w :framework :quiescent-locked)]
                          (repeat count #(run-scenario cfg)))))
              workloads)]
    @(qfor [f fns] (q/task (f)))))


(defmethod run-scenario [:parallel :quiescent-adaptive]
  [{:keys [workloads]}]
  (let [fns (into []
              (mapcat (fn [{:keys [count] :as w}]
                        (let [cfg (assoc w :framework :quiescent-adaptive)]
                          (repeat count #(run-scenario cfg)))))
              workloads)]
    @(qfor [f fns] (q/task (f)))))


(defmethod run-scenario [:parallel :core-async]
  [{:keys [workloads]}]
  (let [fns (into []
              (mapcat (fn [{:keys [count] :as w}]
                        (let [cfg (assoc w :framework :core-async)]
                          (repeat count #(run-scenario cfg)))))
              workloads)]
    @(qfor [f fns] (q/task (f)))))


;; -- Transducer scenarios --

(defmethod run-scenario [:xform :quiescent]
  [{:keys [n producers consumers buffer xf]}]
  (let [ch    (chan buffer xf)
        per-p (quot n producers)
        per-c (quot n consumers)]
    @(qdo
       (qfor [_ (range consumers)]
         (q/task (dotimes [_ per-c] (take! ch))))
       (qfor [_ (range producers)]
         (q/task (dotimes [i per-p] (put! ch i)))))))


(defmethod run-scenario [:xform :core-async]
  [{:keys [n producers consumers buffer xf]}]
  (let [ch  (a/chan buffer xf)
        gos (into
              (mapv (fn [_] (a/go (dotimes [_ (quot n consumers)] (a/<! ch)))) (range consumers))
              (mapv (fn [_] (a/go (dotimes [i (quot n producers)] (a/>! ch i)))) (range producers)))]
    (run! a/<!! gos)))


(defmethod run-scenario [:xform-mapcat :quiescent]
  [{:keys [n producers consumers buffer xf expand-factor]}]
  (let [ch    (chan buffer xf)
        per-p (quot n producers)
        per-c (quot (* n expand-factor) consumers)]
    @(qdo
       (qfor [_ (range consumers)]
         (q/task (dotimes [_ per-c] (take! ch))))
       (qfor [_ (range producers)]
         (q/task (dotimes [i per-p] (put! ch i)))))))


(defmethod run-scenario [:xform-mapcat :core-async]
  [{:keys [n producers consumers buffer xf expand-factor]}]
  (let [ch  (a/chan buffer xf)
        gos (into
              (mapv (fn [_] (a/go (dotimes [_ (quot (* n expand-factor) consumers)] (a/<! ch)))) (range consumers))
              (mapv (fn [_] (a/go (dotimes [i (quot n producers)] (a/>! ch i)))) (range producers)))]
    (run! a/<!! gos)))


(defmethod run-scenario [:xform-filter :quiescent]
  [{:keys [n producers consumers buffer xf pass-ratio]}]
  (let [ch    (chan buffer xf)
        per-p (quot n producers)
        per-c (quot (long (* n pass-ratio)) consumers)]
    @(qdo
       (qfor [_ (range consumers)]
         (q/task (dotimes [_ per-c] (take! ch))))
       (qfor [_ (range producers)]
         (q/task (dotimes [i per-p] (put! ch i)))))))


(defmethod run-scenario [:xform-filter :core-async]
  [{:keys [n producers consumers buffer xf pass-ratio]}]
  (let [ch  (a/chan buffer xf)
        gos (into
              (mapv (fn [_] (a/go (dotimes [_ (quot (long (* n pass-ratio)) consumers)] (a/<! ch)))) (range consumers))
              (mapv (fn [_] (a/go (dotimes [i (quot n producers)] (a/>! ch i)))) (range producers)))]
    (run! a/<!! gos)))


;; -- Pipe scenarios --

(defmethod run-scenario [:pipe :quiescent]
  [{:keys [n producers consumers buffer]}]
  (let [ch-a  (chan buffer)
        ch-b  (chan buffer)
        p     (pipe ch-a ch-b)
        per-p (quot n producers)
        per-c (quot n consumers)]
    @(qdo
       (qfor [_ (range consumers)]
         (q/task (dotimes [_ per-c] (take! ch-b))))
       (q/task
         @(qfor [_ (range producers)]
            (q/task (dotimes [i per-p] (put! ch-a i))))
         (seal! ch-a)
         @p))))


(defmethod run-scenario [:pipe :core-async]
  [{:keys [n producers consumers buffer]}]
  (let [ch-a (a/chan buffer)
        ch-b (a/chan buffer)
        _    (a/pipe ch-a ch-b)
        cs   (mapv (fn [_] (a/go (dotimes [_ (quot n consumers)] (a/<! ch-b)))) (range consumers))
        ps   (mapv (fn [_] (a/go (dotimes [i (quot n producers)] (a/>! ch-a i)))) (range producers))]
    (run! a/<!! ps)
    (a/close! ch-a)
    (run! a/<!! cs)))


(defmethod run-scenario [:pipe-xf :quiescent]
  [{:keys [n producers consumers buffer xf]}]
  (let [ch-a  (chan buffer xf)
        ch-b  (chan buffer)
        p     (pipe ch-a ch-b)
        per-p (quot n producers)
        per-c (quot n consumers)]
    @(qdo
       (qfor [_ (range consumers)]
         (q/task (dotimes [_ per-c] (take! ch-b))))
       (q/task
         @(qfor [_ (range producers)]
            (q/task (dotimes [i per-p] (put! ch-a i))))
         (seal! ch-a)
         @p))))


(defmethod run-scenario [:pipe-xf :core-async]
  [{:keys [n producers consumers buffer xf]}]
  (let [ch-a (a/chan buffer xf)
        ch-b (a/chan buffer)
        _    (a/pipe ch-a ch-b)
        cs   (mapv (fn [_] (a/go (dotimes [_ (quot n consumers)] (a/<! ch-b)))) (range consumers))
        ps   (mapv (fn [_] (a/go (dotimes [i (quot n producers)] (a/>! ch-a i)))) (range producers))]
    (run! a/<!! ps)
    (a/close! ch-a)
    (run! a/<!! cs)))


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
   {:scenario "Pipe 4P→1P→4C" :group :pipe :type :pipe
    :producers 4 :consumers 4 :n 1000000 :buffer 16}
   {:scenario "Pipe XF 4P→1P→4C" :group :pipe :type :pipe-xf :xf (map inc)
    :producers 4 :consumers 4 :n 1000000 :buffer 16}
   {:scenario "Pipe 4P→1P→4C" :group :pipe :type :pipe
    :producers 4 :consumers 4 :n 1000000 :buffer 64}
   {:scenario "Pipe XF 4P→1P→4C" :group :pipe :type :pipe-xf :xf (map inc)
    :producers 4 :consumers 4 :n 1000000 :buffer 64}
   {:scenario "Pipe 4P→1P→4C" :group :pipe :type :pipe
    :producers 4 :consumers 4 :n 1000000 :buffer 1024}
   {:scenario "Pipe XF 4P→1P→4C" :group :pipe :type :pipe-xf :xf (map inc)
    :producers 4 :consumers 4 :n 1000000 :buffer 1024}

   ;; --- Pipeline (parallel contention) ---
   {:scenario "20×Pipe 4P→1P→4C" :group :pipe :type :parallel
    :workloads [{:count 20 :type :pipe :producers 4 :consumers 4 :n 100000 :buffer 64}]}
   {:scenario "20×Pipe 4P→1P→4C" :group :pipe :type :parallel
    :workloads [{:count 20 :type :pipe :producers 4 :consumers 4 :n 100000 :buffer 1024}]}

   ;; --- Fan-in (many producers, 1 consumer) ---
   ;; n must be divisible by producer count to avoid deadlock from truncation
   {:scenario "16P1C"  :group :fan-in :producers 16  :consumers 1 :n 960000  :buffer 64}
   {:scenario "32P1C"  :group :fan-in :producers 32  :consumers 1 :n 960000  :buffer 64}
   {:scenario "64P1C"  :group :fan-in :producers 64  :consumers 1 :n 960000  :buffer 64}
   {:scenario "128P1C" :group :fan-in :producers 128 :consumers 1 :n 960000  :buffer 64}
   {:scenario "16P1C"  :group :fan-in :producers 16  :consumers 1 :n 960000  :buffer 1024}
   {:scenario "32P1C"  :group :fan-in :producers 32  :consumers 1 :n 960000  :buffer 1024}
   {:scenario "64P1C"  :group :fan-in :producers 64  :consumers 1 :n 960000  :buffer 1024}
   {:scenario "128P1C" :group :fan-in :producers 128 :consumers 1 :n 960000  :buffer 1024}

   ;; --- Fan-in XF (lock effect on producer contention) ---
   {:scenario "XF 16P1C"  :group :fan-in-xf :type :xform :xf (map identity)
    :producers 16  :consumers 1 :n 960000  :buffer 64}
   {:scenario "XF 32P1C"  :group :fan-in-xf :type :xform :xf (map identity)
    :producers 32  :consumers 1 :n 960000  :buffer 64}
   {:scenario "XF 64P1C"  :group :fan-in-xf :type :xform :xf (map identity)
    :producers 64  :consumers 1 :n 960000  :buffer 64}
   {:scenario "XF 128P1C" :group :fan-in-xf :type :xform :xf (map identity)
    :producers 128 :consumers 1 :n 960000  :buffer 64}
   {:scenario "XF 16P1C"  :group :fan-in-xf :type :xform :xf (map identity)
    :producers 16  :consumers 1 :n 960000  :buffer 1024}
   {:scenario "XF 32P1C"  :group :fan-in-xf :type :xform :xf (map identity)
    :producers 32  :consumers 1 :n 960000  :buffer 1024}
   {:scenario "XF 64P1C"  :group :fan-in-xf :type :xform :xf (map identity)
    :producers 64  :consumers 1 :n 960000  :buffer 1024}
   {:scenario "XF 128P1C" :group :fan-in-xf :type :xform :xf (map identity)
    :producers 128 :consumers 1 :n 960000  :buffer 1024}])


;; -- Bench harness --

(defn- format-ms
  [v]
  (format "%.3f ms" (* v 1000.0)))


(defn- format-pct
  [v]
  (format "%.1f%%" (* (or v 0) 100.0)))


(defn bench-one
  [cfg framework verbose]
  (let [run-cfg (assoc cfg :framework framework)
        label   (:scenario cfg)
        ch-name (case framework
                  :quiescent "BoundedChannel"
                  :quiescent-locked "Locked"
                  :quiescent-adaptive "Adaptive"
                  :core-async "core.async")]
    (println (str "\nRunning: " label " — " ch-name))
    (let [res         (if verbose
                        (c/with-progress-reporting (c/benchmark (run-scenario run-cfg) {}))
                        (c/benchmark (run-scenario run-cfg) {}))
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


(defn run-all-benchmarks
  [& {:keys [only verbose frameworks]}]
  (let [active       (if only
                       (filterv #(contains? only (:group %)) scenarios)
                       scenarios)
        default-fws  [:quiescent :core-async]
        results      (into []
                       (mapcat (fn [cfg]
                                 (let [fws (or frameworks
                                             (:frameworks cfg)
                                             default-fws)]
                                   (mapv #(bench-one cfg % verbose) fws))))
                       active)
        ;; Compute speedup: relative to fastest variant per scenario+buffer
        pair-key     (fn [r] [(:label r) (:buffer r)])
        min-means    (reduce (fn [acc r]
                               (let [k (pair-key r)
                                     prev (get acc k)]
                                 (if (or (nil? prev) (< (:raw-mean r) prev))
                                   (assoc acc k (:raw-mean r))
                                   acc)))
                       {}
                       results)
        with-speedup (mapv (fn [r]
                             (let [min-mean (get min-means (pair-key r))
                                   ratio    (/ (:raw-mean r) min-mean)]
                               (if (< ratio 1.05)
                                 (assoc r :speedup "")
                                 (assoc r :speedup (format "%.1fx" ratio)))))
                       results)
        cols         [:label :buffer :channel :mean :std-dev :lower-q :upper-q :outlier-var :speedup]]

    (println "\n\n=== BENCHMARK RESULTS ===")
    (pp/print-table cols with-speedup)

    (spit "benchmark_results.md"
      (with-out-str
        (println "## Benchmark Results\n")
        (println (str "|" (str/join "|" (map name cols)) "|"))
        (println (str "|" (str/join "|" (repeat (count cols) "---")) "|"))
        (doseq [row with-speedup]
          (println (str "|" (str/join "|" (map #(get row %) cols)) "|")))))
    with-speedup))


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
        only       (some-> (parse-kw-list args "--only") set)
        frameworks (parse-kw-list args "--frameworks")]
    (run-all-benchmarks :only only :verbose verbose
      :frameworks (seq frameworks))))


(comment

  (def results (run-all-benchmarks))

  (def results (run-all-benchmarks :only #{:transducer}))

  (def results (run-all-benchmarks :only #{:system :transducer} :verbose true))

  ;; Three-way comparison: XADD vs Locked vs core.async
  (def results (run-all-benchmarks
                 :only #{:isolated :small-buffer :fan-in :system}
                 :frameworks [:quiescent :quiescent-locked :core-async]))

  #__)
