(ns quiescent.artifacts
  "Source snapshots and provenance, following Pathling's saved JVM runner."
  (:require [clojure.edn :as edn]
    [clojure.java.io :as io]
    [clojure.java.shell :as shell]
    [clojure.pprint :as pprint]
    [clojure.string :as str])
  (:import [java.lang ProcessHandle]
    [java.lang.management ManagementFactory]
    [java.nio.file AtomicMoveNotSupportedException CopyOption Files StandardCopyOption]
    [java.security MessageDigest]
    [java.time Instant ZoneOffset]
    [java.time.format DateTimeFormatter]
    [java.util UUID]
    [java.util.zip ZipEntry ZipOutputStream]))


(defn- git
  [& args]
  (let [{:keys [exit out err]} (apply shell/sh "git" args)]
    (when-not (zero? exit)
      (throw (ex-info "Cannot record Git revision" {:args args :error err})))
    (str/trimr out)))


(defn- file-hash
  [file]
  (apply str (map #(format "%02x" (bit-and 0xff %))
               (.digest (MessageDigest/getInstance "SHA-256")
                 (Files/readAllBytes (.toPath (io/file file)))))))


(defn- file-hashes
  [paths]
  (into (sorted-map)
    (for [path paths
          ^java.io.File f (file-seq (io/file path))
          :when (.isFile f)
          :when (not= ".DS_Store" (.getName f))]
      [(.getPath f) (file-hash f)])))


(defn context
  []
  (let [runtime (ManagementFactory/getRuntimeMXBean)
        cpu (try (shell/sh "sysctl" "-n" "machdep.cpu.brand_string")
              (catch java.io.IOException _ nil))
        basis-path (System/getProperty "clojure.basis")
        basis (when basis-path (edn/read-string (slurp basis-path)))]
    {:git {:commit (git "rev-parse" "HEAD")
           :description (git "describe" "--tags" "--always" "--dirty")
           :status (git "status" "--porcelain")}
     :source-sha256 (file-hashes ["src" "java"])
     :harness-sha256 (file-hashes ["bench" "bench-test" "test" "tests.edn" "deps.edn" "bb.edn" "build.clj" "dev/channel_bench.clj"])
     :class-sha256 (file-hashes ["target/classes"])
     :loaded-channel (str (.getResource ^Class co.multiply.quiescent.impl.channel.BoundedChannel
                            "BoundedChannel.class"))
     :libraries (into (sorted-map)
                  (map (fn [[lib coord]]
                         [lib (select-keys coord [:mvn/version :git/sha :local/root])]))
                  (:libs basis))
     :environment {:clojure-version (clojure-version)
                   :core-async-resource (str (io/resource "clojure/core/async.clj"))
                   :scheduler-properties (into {} (map (fn [k] [k (System/getProperty k)]))
                                           ["jdk.virtualThreadScheduler.parallelism"
                                            "jdk.virtualThreadScheduler.maxPoolSize"
                                            "clojure.core.async.executor-factory"
                                            "clojure.core.async.go-checking"])
                   :java (into {} (map (fn [k] [k (System/getProperty k)]))
                           ["java.version" "java.vendor" "java.vm.name" "java.vm.version"])
                   :jvm-options (vec (.getInputArguments runtime))
                   :os (into {} (map (fn [k] [k (System/getProperty k)]))
                         ["os.name" "os.arch" "os.version"])
                   :cpu-model (if (.exists (io/file "/proc/cpuinfo"))
                                ;; Some Linux procfs/JDK combinations reject
                                ;; FileInputStream.available(), used by slurp.
                                (some->> (:out (shell/sh "cat" "/proc/cpuinfo"))
                                  (re-find #"(?m)^model name\s*:\s*(.+)$") second)
                                (when (= 0 (:exit cpu)) (str/trim (:out cpu))))
                   :available-processors (.availableProcessors (Runtime/getRuntime))
                   :max-heap-bytes (.maxMemory (Runtime/getRuntime))
                   :gc (mapv #(.getName ^java.lang.management.GarbageCollectorMXBean %)
                         (ManagementFactory/getGarbageCollectorMXBeans))}}))


(defn- edn-string
  [data]
  (binding [*print-length* nil *print-level* nil *print-meta* false]
    (with-out-str (pprint/pprint data))))


(defn- archive-sources!
  [directory ctx]
  (with-open [zip (ZipOutputStream. (io/output-stream (io/file directory "sources.zip")))]
    (doseq [^String path (sort (concat (keys (:source-sha256 ctx)) (keys (:harness-sha256 ctx))))]
      (.putNextEntry zip (ZipEntry. path))
      (io/copy (io/file path) zip)
      (.closeEntry zip))))


(defn write-edn!
  [file data]
  (let [temporary (io/file (str file ".tmp"))
        target (.toPath (io/file file))]
    (spit temporary (edn-string data))
    (try
      (Files/move (.toPath temporary) target
        (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING
                                StandardCopyOption/ATOMIC_MOVE]))
      (catch AtomicMoveNotSupportedException _
        (Files/move (.toPath temporary) target
          (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING]))))))


(defn create-output!
  "Reserve a new output directory. Never reuse an existing result directory."
  [opts ctx]
  (let [stamp (.format (.withZone (DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmss'Z'") ZoneOffset/UTC)
                (Instant/now))
        directory (io/file (or (:output opts)
                             (str "benchmarks/results/" stamp "-"
                               (subs (get-in ctx [:git :commit]) 0 8) "-"
                               (subs (str (UUID/randomUUID)) 0 8))))]
    (io/make-parents directory)
    (Files/createDirectory (.toPath directory) (make-array java.nio.file.attribute.FileAttribute 0))
    directory))


(defn save-sources!
  [directory ctx]
  (spit (io/file directory "git.patch") (git "diff" "HEAD" "--binary" "--" "."))
  (archive-sources! directory ctx))


(defn process-identity
  []
  {:pid (.pid (ProcessHandle/current))
   :started-at (str (Instant/ofEpochMilli (.getStartTime (ManagementFactory/getRuntimeMXBean))))})
