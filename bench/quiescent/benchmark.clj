(ns quiescent.benchmark
  (:refer-clojure :exclude [run!])
  (:require [clojure.edn :as edn]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [quiescent.allocation :as allocation]
    [quiescent.artifacts :as artifacts]
    [quiescent.scenarios :as scenarios]
    [quiescent.workload :as workload])
  (:import [java.lang.management ManagementFactory]
    [java.time Instant]
    [java.util Locale]
    [java.util.concurrent TimeUnit]))


(def profiles
  {:full {:warmup-ms 10000 :warmup-min 3 :samples 12 :allocation-samples 5 :small? false}
   :quick {:warmup-ms 1000 :warmup-min 1 :samples 3 :allocation-samples 3 :small? false}
   :smoke {:warmup-ms 0 :warmup-min 1 :samples 2 :allocation-samples 1 :small? true}})


(defn options
  [requested]
  (let [opts (merge {:profile :full :suite :primary :forks 4 :timeout-ms 120000
                     :frameworks [:quiescent :core-async]} requested)
        allowed #{:profile :suite :forks :timeout-ms :frameworks :cases :output :label :list?}]
    (when-let [unknown (seq (remove allowed (keys opts)))]
      (throw (ex-info "Unknown benchmark options" {:unknown unknown})))
    (when-not (contains? profiles (:profile opts))
      (throw (ex-info "Unknown profile" {:profile (:profile opts)})))
    (when-not (#{:primary :all} (:suite opts))
      (throw (ex-info "Unknown suite" {:suite (:suite opts)})))
    (doseq [k [:forks :timeout-ms]]
      (when-not (and (integer? (opts k)) (pos? (opts k)))
        (throw (ex-info "Option must be a positive integer" {:option k}))))
    (doseq [[k valid] [[:frameworks #{:quiescent :core-async}] [:cases (set scenarios/ids)]]]
      (when (contains? opts k)
        (let [xs (opts k)]
          (when-not (and (vector? xs) (seq xs) (= (count xs) (count (distinct xs)))
                      (every? valid xs))
            (throw (ex-info "Invalid benchmark selection" {:option k :value xs :available valid}))))))
    (doseq [k [:label :output]]
      (when (and (contains? opts k) (not (and (string? (opts k)) (seq (opts k)))))
        (throw (ex-info "Option must be a nonempty string" {:option k}))))
    (when (and (contains? opts :list?) (not (boolean? (:list? opts))))
      (throw (ex-info ":list? must be boolean" {})))
    opts))


(defn selected-cases
  [opts]
  (let [selection (or (some-> (:cases opts) set)
                    (if (= :primary (:suite opts)) scenarios/primary (set scenarios/ids)))]
    (filterv #(selection (:id %)) scenarios/cases)))


(defn schedule
  [opts]
  (vec (for [fork (range 1 (inc (:forks opts)))
             spec (selected-cases opts)
             framework (if (odd? fork) (:frameworks opts) (reverse (:frameworks opts)))]
         {:fork fork :case (:id spec) :framework framework
          :directory (str "fork-" fork "/" (name (:id spec)) "/" (name framework))})))


(defn median
  [xs]
  (let [v (vec (sort xs)) n (count v) m (quot n 2)]
    (if (odd? n) (v m) (/ (+ (v (dec m)) (v m)) 2.0))))


(defn mean
  [xs]
  (/ (double (reduce + xs)) (count xs)))


(defn fmt
  [pattern & args]
  (String/format Locale/ROOT pattern (to-array args)))


(defn summaries
  [runs]
  (mapv (fn [[[id framework] rs]]
          (let [means (mapv :mean-ns rs) mid (median means)]
            {:case id :framework framework :means-ns means :median-ns mid
             :min-ns (apply min means) :max-ns (apply max means)
             :spread-percent (when (> (count means) 1)
                               (* 100.0 (/ (- (apply max means) (apply min means)) mid)))}))
    (sort-by (comp pr-str key) (group-by (juxt :case :framework) runs))))


(defn summary
  [batch]
  (str "# JVM channel baseline\n\nStatus: `" (name (:status batch)) "`. Profile: `"
    (name (get-in batch [:options :profile])) "`. Completed " (count (:runs batch)) " / "
    (count (:schedule batch)) " fresh JVMs.\n\n"
    (when (not= :full (get-in batch [:options :profile]))
      "Exploratory measurements; not a performance baseline.\n\n")
    "Primary metric: common start release through all transfer/drain work, including scheduler waits, "
    "transducers, checksum/count accumulation, channel closing and wakeups. Channel, payload and worker creation, "
    "validation and task teardown are outside timing. These are finite-batch durations, not per-operation latency.\n\n"
    "Each row summarizes independent JVM means. All raw trials and JVMs are retained. Spread is "
    "(max - min) / median; it is not a confidence interval. One fork does not establish JVM variability.\n\n"
    "| Case | Implementation | JVMs | Median ms | Min ms | Max ms | Spread % |\n"
    "|---|---|---:|---:|---:|---:|---:|\n"
    (apply str
      (for [{:keys [case framework means-ns median-ns min-ns max-ns spread-percent]} (summaries (:runs batch))]
        (fmt "| %s | %s | %d | %.3f | %.3f | %.3f | %s |\n"
          (name case) (name framework) (count means-ns)
          (/ median-ns 1e6) (/ min-ns 1e6) (/ max-ns 1e6)
          (if spread-percent (fmt "%.2f" spread-percent) "—"))))
    "\n## Comparison\n\nRatio is core.async / Quiescent elapsed time (>1 favors Quiescent). "
    "Per-fork ratios pair adjacent implementation workers; ratio of medians is reported separately.\n\n"
    "| Case | Ratio of medians | Per-fork ratios |\n|---|---:|---|\n"
    (apply str
      (for [[id rs] (sort-by (comp name key) (group-by :case (:runs batch)))
            :let [q (filter #(= :quiescent (:framework %)) rs)
                  a (filter #(= :core-async (:framework %)) rs)
                  pairs (keep (fn [[_ rows]]
                                (let [arms (into {} (map (juxt :framework :mean-ns)) rows)]
                                  (when (= 2 (count arms)) (/ (:core-async arms) (:quiescent arms)))))
                          (sort-by key (group-by :fork rs)))]
            :when (and (seq q) (seq a))]
        (fmt "| %s | %.3fx | %s |\n" (name id)
          (/ (median (map :mean-ns a)) (median (map :mean-ns q)))
          (str/join ", " (map #(fmt "%.3fx" %) pairs)))))
    "\n## Individual JVM observations\n\n| Fork | Case | Implementation | Mean ms | Details |\n"
    "|---:|---|---|---:|---|\n"
    (apply str (for [{:keys [fork case framework mean-ns directory]} (:runs batch)]
                 (fmt "| %d | %s | %s | %.3f | [Trials](%s/summary.md) |\n"
                   fork (name case) (name framework) (/ mean-ns 1e6) directory)))
    "\nPer-worker run.edn files retain samples, channel completion times, consumer counts, CPU/GC counters, "
    "warmup, exact pre/postflight validation and process identity. CPU/GC observations bracket the timed phase "
    "and include some coordination/measurement overhead. Allocation uses separate whole-JVM passes.\n\n"
    "## Allocated heap bytes\n\nEach observation is a separate allocation pass after timing. "
    "Transfer includes release through worker teardown; setup includes payloads/channels/workers. "
    "All JVM threads are counted, including background activity; controls are not subtracted.\n\n"
    "| Fork | Case | Implementation | Transfer bytes/delivered value | Setup bytes/trial |\n"
    "|---:|---|---|---:|---:|\n"
    (apply str
      (for [{:keys [fork case framework allocation]} (:runs batch)]
        (fmt "| %d | %s | %s | %s | %s |\n" fork (name case) (name framework)
          (if-let [n (:mean-transfer-bytes-per-value allocation)] (fmt "%.2f" n) "unsupported")
          (if-let [n (:mean-setup-bytes allocation)] (fmt "%.0f" n) "unsupported"))))))


(defn worker-summary
  [run]
  (str "# Channel workload trials\n\nStatus: `" (name (:status run)) "`. Case: `"
    (name (get-in run [:request :entry :case])) "`. Implementation: `"
    (name (get-in run [:request :entry :framework])) "`.\n\n"
    "Completion median/max describes whole workloads finishing from the common start, not operation latency. "
    "Mixed topologies have different amounts of work, so their completion spread is not a fairness score. "
    "CPU and GC counter deltas bracket transfer with a small amount of measurement overhead.\n\n"
    "| Trial | Elapsed ms | Delivered values/s | Workload completion median ms | Max ms | Process CPU ms | GC ms |\n"
    "|---:|---:|---:|---:|---:|---:|---:|\n"
    (apply str
      (for [[i sample] (map-indexed vector (:samples run))
            :let [times (map :elapsed-ns (:workloads sample))
                  before (:counters-before sample) after (:counters-after sample)
                  cpu-before (:process-cpu-ns before) cpu-after (:process-cpu-ns after)
                  cpu (when (and cpu-before cpu-after (<= 0 cpu-before cpu-after)) (- cpu-after cpu-before))
                  gc-times (fn [c] (map :ms (vals (:gc c))))
                  gc (when (every? #(<= 0 %) (concat (gc-times before) (gc-times after)))
                       (- (reduce + (gc-times after)) (reduce + (gc-times before))))]]
        (fmt "| %d | %.3f | %.0f | %.3f | %.3f | %s | %s |\n"
          (inc i) (/ (:elapsed-ns sample) 1e6)
          (/ (* 1e9 (reduce + (map :count (:workloads sample)))) (:elapsed-ns sample))
          (/ (median times) 1e6) (/ (apply max times) 1e6)
          (if cpu (fmt "%.3f" (/ cpu 1e6)) "unsupported")
          (if gc (str gc) "unsupported"))))
    "\n## Separate allocation passes\n\nThese use the JVM-wide allocated-byte counter; "
    "transfer spans gate release through worker teardown, before result validation. "
    "The scope includes harness/runtime/background allocations, not just channel objects.\n\n"
    "| Pass | Transfer bytes | Bytes/delivered value | Setup bytes | Setup + transfer bytes |\n"
    "|---:|---:|---:|---:|---:|\n"
    (apply str
      (for [[i sample] (map-indexed vector (get-in run [:allocation :samples]))
            :let [a (:allocation sample) n (reduce + (map :count (:workloads sample)))]]
        (fmt "| %d | %d | %.2f | %d | %d |\n" (inc i) (:transfer-bytes a)
          (/ (double (:transfer-bytes a)) n) (:setup-bytes a) (:setup-and-transfer-bytes a))))))


(defn checkpoint!
  [directory state]
  (artifacts/write-edn! (io/file directory "batch.edn") @state)
  (spit (io/file directory "summary.md") (summary @state)))


(defn worker!
  [{:keys [options entry context] :as request} directory]
  (let [spec (first (filter #(= (:case entry) (:id %)) scenarios/cases))
        settings (profiles (:profile options))
        params (assoc settings :timeout-ms (:timeout-ms options))
        state (atom {:schema-version 1 :status :running :request request :spec spec
                     :process (artifacts/process-identity) :context (artifacts/context)
                     :settings settings :warmup [] :samples []})
        save! #(do (artifacts/write-edn! (io/file directory "run.edn") @state)
                 (spit (io/file directory "summary.md") (worker-summary @state)))]
    (save!)
    (try
      (when-not (= context (:context @state))
        (throw (ex-info "Worker source or runtime differs from coordinator" {})))
      (workload/execute! spec (:framework entry) (assoc params :small? true :exact? true))
      (swap! state assoc :preflight :passed)
      (loop [elapsed 0 i 0]
        (when (or (< elapsed (* 1000000 (:warmup-ms settings))) (< i (:warmup-min settings)))
          (let [sample (workload/execute! spec (:framework entry) params)]
            (swap! state update :warmup conj sample)
            (save!)
            (recur (+ elapsed (:elapsed-ns sample)) (inc i)))))
      (dotimes [i (:samples settings)]
        (let [sample (workload/execute! spec (:framework entry) params)]
          (swap! state update :samples conj sample)
          (save!)
          (println "sample" (inc i) (fmt "%.3f ms" (/ (:elapsed-ns sample) 1e6)))
          (flush)))
      (if-let [bean (allocation/bean)]
        (let [probe (allocation/probe! bean)
              samples (mapv (fn [_]
                              (workload/execute! spec (:framework entry) (assoc params :allocation-bean bean)))
                        (range (:allocation-samples settings)))
              per-value (fn [sample]
                          (/ (double (get-in sample [:allocation :transfer-bytes]))
                            (reduce + (map :count (:workloads sample)))))]
          (swap! state assoc :allocation
            {:status :measured :method :thread-mx-bean-total :scope :all-jvm-threads
             :probe probe :samples samples
             :mean-transfer-bytes-per-value (mean (map per-value samples))
             :mean-setup-bytes (mean (map #(get-in % [:allocation :setup-bytes]) samples))})
          (save!))
        (swap! state assoc :allocation {:status :unsupported :method :thread-mx-bean-total}))
      (workload/execute! spec (:framework entry) (assoc params :small? true :exact? true))
      (when-not (= context (artifacts/context))
        (throw (ex-info "Source or runtime changed during measurement" {})))
      (swap! state assoc :status :complete :postflight :passed :finished-at (str (Instant/now))
        :mean-ns (mean (map :elapsed-ns (:samples @state))))
      (save!)
      (catch Throwable e
        (swap! state assoc :status :failed :error (str e))
        (save!)
        (throw e)))))


(defn worker-command
  [request-file directory]
  (into [(.getPath (io/file (System/getProperty "java.home") "bin" "java"))]
    (concat (.getInputArguments (ManagementFactory/getRuntimeMXBean))
      ["-cp" (System/getProperty "java.class.path") "clojure.main" "-m" "quiescent.benchmark"
       (.getAbsolutePath (io/file request-file)) (.getAbsolutePath (io/file directory))])))


(defn run-worker!
  [request-file directory timeout-ms]
  (let [command (worker-command request-file directory)
        process (.start (doto (ProcessBuilder. ^java.util.List command)
                          (.redirectErrorStream true)
                          (.redirectOutput (io/file directory "console.log"))))
        stop #(when (.isAlive process) (.destroyForcibly process))
        hook (Thread. ^Runnable stop)]
    (.addShutdownHook (Runtime/getRuntime) hook)
    (try
      (let [finished? (.waitFor process (long timeout-ms) TimeUnit/MILLISECONDS)]
        (when-not finished? (stop) (.waitFor process))
        {:pid (.pid process) :exit-status (.exitValue process) :command command
         :timed-out? (not finished?)})
      (finally
        (stop)
        (try (.removeShutdownHook (Runtime/getRuntime) hook)
          (catch IllegalStateException _ nil))))))


(defn validate-worker!
  [run request pid]
  (let [settings (profiles (get-in request [:options :profile]))
        checks {:complete (= :complete (:status run))
                :preflight (= :passed (:preflight run))
                :postflight (= :passed (:postflight run))
                :request (= request (:request run))
                :context (= (:context request) (:context run))
                :process (= pid (get-in run [:process :pid]))
                :settings (= settings (:settings run))
                :spec (= (first (filter #(= (get-in request [:entry :case]) (:id %)) scenarios/cases)) (:spec run))
                :samples (= (:samples settings) (count (:samples run)))
                :timings (every? #(and (number? (:elapsed-ns %)) (pos? (:elapsed-ns %))) (:samples run))
                :validation (every? #(= :count-and-sum (:validation %)) (:samples run))
                :warmup (and (>= (count (:warmup run)) (:warmup-min settings))
                          (>= (reduce + (map :elapsed-ns (:warmup run))) (* 1000000 (:warmup-ms settings))))
                :mean (= (:mean-ns run) (mean (map :elapsed-ns (:samples run))))}]
    (when-not (or (= :unsupported (get-in run [:allocation :status]))
                (and (= :measured (get-in run [:allocation :status]))
                  (= :passed (get-in run [:allocation :probe :status]))
                  (= (:allocation-samples settings) (count (get-in run [:allocation :samples])))
                  (every? (fn [sample]
                            (let [{:keys [setup-bytes transfer-bytes setup-and-transfer-bytes]} (:allocation sample)]
                              (and (= :count-and-sum (:validation sample))
                                (every? #(and (number? %) (<= 0 %)) [setup-bytes transfer-bytes setup-and-transfer-bytes])
                                (= (+ setup-bytes transfer-bytes) setup-and-transfer-bytes))))
                    (get-in run [:allocation :samples]))))
      (throw (ex-info "Invalid allocation result" {})))
    (when-not (every? true? (vals checks))
      (throw (ex-info "Invalid worker result" {:checks checks})))
    run))


(defn run!
  [requested]
  (let [opts (options requested) plan (schedule opts)]
    (if (:list? opts)
      (do (doseq [spec (selected-cases opts)] (prn (select-keys spec [:id :scenario :group])))
        (println (count plan) "fresh JVMs;" (:forks opts) "forks per implementation/case"))
      (let [context (artifacts/context)
            directory (artifacts/create-output! opts context)
            state (atom {:schema-version 1 :status :running :options opts :context context
                         :started-at (str (Instant/now)) :process (artifacts/process-identity)
                         :schedule plan :runs []})]
        (artifacts/save-sources! directory context)
        (checkpoint! directory state)
        (try
          (doseq [[i entry] (map-indexed vector plan)]
            (let [dir (io/file directory (:directory entry))
                  request {:options opts :entry entry :context context}
                  request-file (io/file dir "request.edn")]
              (.mkdirs dir)
              (artifacts/write-edn! request-file request)
              (swap! state assoc :active-run entry)
              (checkpoint! directory state)
              (println (str "[" (inc i) "/" (count plan) "]") entry)
              (flush)
              (let [process (run-worker! request-file dir
                              (+ 300000 (* 3 (:timeout-ms opts) (+ 4 (:samples (profiles (:profile opts)))))))]
                (artifacts/write-edn! (io/file dir "process.edn") process)
                (when-not (zero? (:exit-status process))
                  (throw (ex-info "Worker failed; inspect console.log" {:directory (str dir) :process process})))
                (let [run (validate-worker! (edn/read-string (slurp (io/file dir "run.edn"))) request (:pid process))]
                  (swap! state update :runs conj
                    (assoc entry :mean-ns (:mean-ns run) :process process
                      :allocation (dissoc (:allocation run) :samples)))
                  (swap! state dissoc :active-run)
                  (checkpoint! directory state)))))
          (swap! state assoc :status :complete :finished-at (str (Instant/now)))
          (checkpoint! directory state)
          (println "Saved" (str directory))
          {:status :complete :output (str directory)}
          (catch Throwable e
            (swap! state assoc :status :failed :error (str e))
            (checkpoint! directory state)
            (throw e)))))))


(defn -main
  [request-file directory]
  (try
    (worker! (edn/read-string (slurp request-file)) directory)
    (System/exit 0)
    (catch Throwable e
      (.printStackTrace e)
      (System/exit 1))))
