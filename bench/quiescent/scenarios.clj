(ns quiescent.scenarios)


;; Plain-channel pressure topologies. Specifications are persisted with each
;; capture; transducer cases remain only in historical source snapshots.
(def scenarios
  [;; ================================================================
   ;; Isolated (single channel, profiling baseline)
   ;; ================================================================
   {:scenario "1P1C" :group :isolated :producers 1 :consumers 1 :n 1000000 :buffer 1024}
   {:scenario "1P1C" :group :isolated :producers 1 :consumers 1 :n 1000000 :buffer 1}
   {:scenario "4P4C" :group :isolated :producers 4 :consumers 4 :n 1000000 :buffer 1024}
   {:scenario "Ping-pong" :group :isolated :type :ping-pong :n 100000 :buffer 1}

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
   ;; Optimal conditions (buf=1024 — does scaling the consumer side
   ;; help when producers saturate the buffer?)
   ;; n must be divisible by producer count
   ;; ================================================================
   {:scenario  "24×128P1C" :group :optimal :buffer 1024
    :workloads [{:count 24 :producers 128 :consumers 1 :n 96000}]}
   {:scenario  "24×128P16C" :group :optimal :buffer 1024
    :workloads [{:count 24 :producers 128 :consumers 16 :n 96000}]}

   ;; ================================================================
   ;; Saturated pipeline (pipe under saturation)
   ;; ================================================================
   {:scenario  "20×Pipe 4P→4C" :group :saturated-pipe :buffer 64
    :workloads [{:count 20 :type :pipe :producers 4 :consumers 4 :n 100000}]}])


(def ids
  [:single :single-buffer-one :single-mpmc :ping-pong
   :fifty :fifty-mpmc :two-hundred :two-hundred-buffer-one :mixed
   :fan-in-32 :fan-in-128 :fan-out-32 :fan-out-128
   :wide-fan-in :wide-mpmc :pipe])


(def cases (mapv #(assoc %1 :id %2) scenarios ids))


(def primary
  #{:single :fifty :fifty-mpmc :two-hundred-buffer-one :mixed
    :fan-in-32 :fan-out-32 :pipe})


(defn workloads
  [spec small?]
  (vec
    (mapcat
      (fn [w]
        (let [w (merge {:type :throughput :producers 1 :consumers 1}
                  (dissoc spec :workloads :id :scenario :group) w)
              p (:producers w)
              w (if small? (assoc w :n (* p 16)) w)]
          (when (contains? w :xf)
            (throw (ex-info "Transducer workloads are not supported by the plain-channel benchmark" w)))
          (when-not (zero? (mod (:n w) p))
            (throw (ex-info "Message count must be divisible by producer count" w)))
          (repeat (:count w 1) (dissoc w :count))))
      (or (:workloads spec) [{}]))))


(defn expected-values
  [{:keys [n]}]
  (range n))
