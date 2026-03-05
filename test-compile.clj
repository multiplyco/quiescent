(require '[babashka.process :as p])
(p/sh "clojure" "-T:build" "javac")
