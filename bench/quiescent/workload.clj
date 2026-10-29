(ns quiescent.workload
  "Prepared, bounded channel workloads. Only transfer/drain is timed."
  (:require [clojure.core.async :as a]
    [clojure.walk :as walk]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.channel :as ch]
    [quiescent.allocation :as allocation]
    [quiescent.scenarios :as scenarios])
  (:import [java.util.concurrent CountDownLatch TimeUnit]
    [java.lang.management ManagementFactory]))


(defn await!
  [^CountDownLatch latch timeout-ms phase]
  (when-not (.await latch (long timeout-ms) TimeUnit/MILLISECONDS)
    (throw (ex-info "Channel workload timed out" {:phase phase :timeout-ms timeout-ms}))))


;; Expand two versions of the same loops. core.async operations must remain
;; lexically inside go; wrapping them in ordinary functions changes semantics.
;; The timed loops have no clock reads or shared counters per message.
(defmacro launch
  [framework context actor]
  (let [body
        '(let [{:keys [role source sink values remaining collect?]} actor]
           (try
             (.countDown ^CountDownLatch (:ready context))
             (START)
             (let [result
                   (case role
                     :producer
                     (do
                       (doseq [v values]
                         (when-not (PUT sink v)
                           (throw (ex-info "Premature channel close" {}))))
                       (when (zero? (.decrementAndGet ^java.util.concurrent.atomic.AtomicInteger remaining))
                         (CLOSE sink))
                       {:role role})

                     :consumer
                     (loop [n 0 sum 0 collected (when collect? (transient []))]
                       (let [v (TAKE source)]
                         (if (END? v)
                           {:role role :count n :sum sum
                            :values (when collect? (persistent! collected))}
                           (recur (inc n) (unchecked-add sum (long v))
                             (when collect? (conj! collected v))))))

                     :relay
                     (do
                       (loop []
                         (let [v (TAKE source)]
                           (when-not (END? v)
                             (when-not (PUT sink v)
                               (throw (ex-info "Premature relay close" {})))
                             (recur))))
                       (CLOSE sink)
                       {:role role})

                     :pong
                     (do (dotimes [_ (count values)]
                           (let [v (TAKE source)]
                             (when (END? v) (throw (ex-info "Premature pong close" {})))
                             (when-not (PUT sink v) (throw (ex-info "Premature pong close" {})))))
                       {:role role})

                     :ping
                     (loop [vs (seq values) n 0 sum 0 collected (when collect? (transient []))]
                       (if-let [vs vs]
                         (do
                           (when-not (PUT sink (first vs))
                             (throw (ex-info "Premature ping close" {})))
                           (let [v (TAKE source)]
                             (when (END? v) (throw (ex-info "Premature ping close" {})))
                             (recur (next vs) (inc n) (unchecked-add sum (long v))
                               (when collect? (conj! collected v)))))
                         {:role role :count n :sum sum
                          :values (when collect? (persistent! collected))})))]
               (aset ^objects (:results context) (int (:index actor))
                 (assoc result :workload (:workload actor) :finished-ns (System/nanoTime))))
             (catch Throwable e
               (aset ^objects (:results context) (int (:index actor)) {:error e}))
             (finally (.countDown ^CountDownLatch (:done context)))))
        expand (fn [model]
                 (walk/postwalk
                   (fn [form]
                     (if (seq? form)
                       (let [[op & args] form]
                         (case op
                           START (if (= model :quiescent)
                                   '(.await ^CountDownLatch (:gate context))
                                   '(a/<! (:gate context)))
                           PUT (if (= model :quiescent)
                                 `(ch/put! ~@args false) `(a/>! ~@args))
                           TAKE (if (= model :quiescent)
                                  `(ch/poll [v# ~@args] v# ::closed) `(a/<! ~@args))
                           CLOSE (if (= model :quiescent) `(ch/seal! ~@args) `(a/close! ~@args))
                           END? (if (= model :quiescent) `(= ::closed ~@args) `(nil? ~@args))
                           form))
                       form)) body))]
    `(let [~'context ~context ~'actor ~actor]
       (if (= ~framework :quiescent)
         (q/task ~(expand :quiescent))
         (a/go ~(expand :core-async))))))


(defn prepare
  [spec framework collect? small?]
  (let [specs (scenarios/workloads spec small?)
        channels (atom [])
        make-channel (fn [buffer xf]
                       (let [c (if (= framework :quiescent)
                                 (if xf (ch/chan buffer (scenarios/transducer xf)) (ch/chan buffer))
                                 (if xf (a/chan buffer (scenarios/transducer xf)) (a/chan buffer)))]
                         (swap! channels conj c)
                         c))
        actors (vec
                 (mapcat
                   (fn [[id {:keys [type n buffer producers consumers xf]}]]
                     (let [source (make-channel buffer xf)
                           sink (if (#{:pipe :ping-pong} type) (make-channel buffer nil) source)
                           values (vec (range n))
                           base {:workload id :collect? collect?}
                           remaining (java.util.concurrent.atomic.AtomicInteger. producers)]
                       (map #(merge base %)
                         (if (= type :ping-pong)
                           [{:role :ping :source sink :sink source :values values}
                            {:role :pong :source source :sink sink :values values}]
                           (concat
                             (repeat consumers {:role :consumer :source sink})
                             (when (= type :pipe) [{:role :relay :source source :sink sink}])
                             (for [p (range producers)]
                               {:role :producer :sink source :remaining remaining
                                :values (subvec values (* p (quot n producers))
                                          (* (inc p) (quot n producers)))}))))))
                   (map-indexed vector specs)))
        ;; One start gate per workload avoids core.async's 1024 pending-take
        ;; limit on a single control channel. Both arms release the same gates
        ;; in the same order; that bounded broadcast is included in timing.
        gates (mapv (fn [_] (if (= framework :quiescent) (CountDownLatch. 1) (a/chan))) specs)
        context {:ready (CountDownLatch. (count actors))
                 :done (CountDownLatch. (count actors))
                 :gates gates
                 :results (object-array (count actors))}
        handles (mapv (fn [i actor]
                        (launch framework (assoc context :gate (gates (:workload actor))) (assoc actor :index i)))
                  (range) actors)]
    (assoc context :handles handles :channels @channels :framework framework :specs specs)))


(defn counters
  []
  (let [os (ManagementFactory/getOperatingSystemMXBean)]
    {:process-cpu-ns (when (instance? com.sun.management.OperatingSystemMXBean os)
                       (.getProcessCpuTime ^com.sun.management.OperatingSystemMXBean os))
     :gc (into {} (map (fn [^java.lang.management.GarbageCollectorMXBean gc]
                         [(.getName gc) {:count (.getCollectionCount gc)
                                         :ms (.getCollectionTime gc)}])
                    (ManagementFactory/getGarbageCollectorMXBeans)))}))


(defn validate!
  [specs results exact?]
  (when-let [error (some :error results)] (throw error))
  (mapv
    (fn [id spec]
      (let [rs (filter #(and (= id (:workload %)) (#{:consumer :ping} (:role %))) results)
            expected (when exact? (scenarios/expected-values spec))
            input-n (:n spec)
            even-n (quot (inc input-n) 2)
            input-sum (quot (* input-n (dec input-n)) 2)
            [expected-n expected-sum]
            (case (:xf spec)
              :map [input-n (+ input-sum input-n)]
              :filter [even-n (* even-n (dec even-n))]
              :mapcat [(* 2 input-n) (* 2 input-sum)]
              [input-n input-sum])
            n (reduce + (map :count rs))
            sum (reduce + (map :sum rs))]
        (when-not (and (= expected-n n) (= expected-sum sum)
                    (or (not exact?) (= (frequencies expected) (frequencies (mapcat :values rs)))))
          (throw (ex-info "Channel workload produced incorrect results"
                   {:workload id :spec spec :count n :sum sum :exact? exact?})))
        {:workload id :count n :sum sum :finished-ns (apply max (map :finished-ns rs))
         :consumer-counts (mapv :count rs)}))
    (range) specs))


(defn execute!
  "Create workers/payloads before timing. Time release through last actor's
   transfer/drain completion. Validate and await task teardown after timing."
  [spec framework {:keys [timeout-ms exact? small? allocation-bean] :or {timeout-ms 120000}}]
  (let [before-setup (allocation/total allocation-bean)
        {:keys [ready done gates results handles channels specs]}
        (prepare spec framework exact? small?)]
    (try
      (await! ready timeout-ms :ready)
      (let [before (counters)
            before-transfer (allocation/total allocation-bean)
            start (System/nanoTime)]
        (doseq [gate gates]
          (if (= framework :quiescent) (.countDown ^CountDownLatch gate) (a/close! gate)))
        (await! done timeout-ms :transfer)
        (doseq [h handles]
          (if (= framework :quiescent) (q/deref-cpu h) (a/<!! h)))
        (let [after-transfer (allocation/total allocation-bean)
              after (counters)
              rs (vec results)
              workloads (validate! specs rs exact?)
              finish (apply max (map :finished-ns rs))]
          {:elapsed-ns (- finish start)
           :allocation (when allocation-bean
                         {:setup-bytes (- before-transfer before-setup)
                          :transfer-bytes (- after-transfer before-transfer)
                          :setup-and-transfer-bytes (- after-transfer before-setup)})
           :counters-before before :counters-after after
           :workloads (mapv #(-> % (assoc :elapsed-ns (- (:finished-ns %) start))
                               (dissoc :finished-ns)) workloads)
           :validation (if exact? :exact :count-and-sum)}))
      (catch Throwable e
        (throw (ex-info "Channel benchmark failed"
                 {:case (:id spec) :framework framework :cause (ex-data e)
                  :actor-errors (mapv #(str (:error %)) (filter :error (vec results)))} e)))
      (finally
        (doseq [gate gates]
          (if (= framework :quiescent) (.countDown ^CountDownLatch gate) (a/close! gate)))
        (doseq [c channels]
          (if (= framework :quiescent) (ch/cancel! c) (a/close! c)))))))
