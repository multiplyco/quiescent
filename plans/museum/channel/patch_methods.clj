(defmethod run-scenario [:xform :quiescent-locked]
  [{:keys [n producers consumers buffer xf]}]
  (let [ch    (BoundedChannelLockedXf. (int buffer) xf)
        per-p (quot n producers)
        per-c (quot n consumers)]
    @(qdo
       (qfor [_ (range consumers)]
         (q/task (dotimes [_ per-c] (take! ch))))
       (qfor [_ (range producers)]
         (q/task (dotimes [i per-p] (put! ch i)))))))

(defmethod run-scenario [:xform :quiescent-adaptive]
  [{:keys [n producers consumers buffer xf]}]
  (let [ch    (BoundedChannelAdaptiveXf. (int buffer) xf)
        per-p (quot n producers)
        per-c (quot n consumers)]
    @(qdo
       (qfor [_ (range consumers)]
         (q/task (dotimes [_ per-c] (take! ch))))
       (qfor [_ (range producers)]
         (q/task (dotimes [i per-p] (put! ch i)))))))

(defmethod run-scenario [:xform-mapcat :quiescent-locked]
  [{:keys [n producers consumers buffer xf expand-factor]}]
  (let [ch    (BoundedChannelLockedXf. (int buffer) xf)
        per-p (quot n producers)
        per-c (quot (* n expand-factor) consumers)]
    @(qdo
       (qfor [_ (range consumers)]
         (q/task (dotimes [_ per-c] (take! ch))))
       (qfor [_ (range producers)]
         (q/task (dotimes [i per-p] (put! ch i)))))))

(defmethod run-scenario [:xform-mapcat :quiescent-adaptive]
  [{:keys [n producers consumers buffer xf expand-factor]}]
  (let [ch    (BoundedChannelAdaptiveXf. (int buffer) xf)
        per-p (quot n producers)
        per-c (quot (* n expand-factor) consumers)]
    @(qdo
       (qfor [_ (range consumers)]
         (q/task (dotimes [_ per-c] (take! ch))))
       (qfor [_ (range producers)]
         (q/task (dotimes [i per-p] (put! ch i)))))))

(defmethod run-scenario [:xform-filter :quiescent-locked]
  [{:keys [n producers consumers buffer xf pass-ratio]}]
  (let [ch    (BoundedChannelLockedXf. (int buffer) xf)
        per-p (quot n producers)
        per-c (quot (long (* n pass-ratio)) consumers)]
    @(qdo
       (qfor [_ (range consumers)]
         (q/task (dotimes [_ per-c] (take! ch))))
       (qfor [_ (range producers)]
         (q/task (dotimes [i per-p] (put! ch i)))))))

(defmethod run-scenario [:xform-filter :quiescent-adaptive]
  [{:keys [n producers consumers buffer xf pass-ratio]}]
  (let [ch    (BoundedChannelAdaptiveXf. (int buffer) xf)
        per-p (quot n producers)
        per-c (quot (long (* n pass-ratio)) consumers)]
    @(qdo
       (qfor [_ (range consumers)]
         (q/task (dotimes [_ per-c] (take! ch))))
       (qfor [_ (range producers)]
         (q/task (dotimes [i per-p] (put! ch i)))))))

(defmethod run-scenario [:pipe-xf :quiescent-locked]
  [{:keys [n producers consumers buffer xf]}]
  (let [ch-a  (BoundedChannelLockedXf. (int buffer) xf)
        ch-b  (BoundedChannelLocked. (int buffer))
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

(defmethod run-scenario [:pipe-xf :quiescent-adaptive]
  [{:keys [n producers consumers buffer xf]}]
  (let [ch-a  (BoundedChannelAdaptiveXf. (int buffer) xf)
        ch-b  (BoundedChannelAdaptive. (int buffer))
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
