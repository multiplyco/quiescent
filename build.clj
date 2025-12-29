(ns build
  (:require
    [clojure.string :as str]
    [clojure.tools.build.api :as b]
    [deps-deploy.deps-deploy :as deploy]))

(def lib 'co.multiply/quiescent)
(def version "0.1.4")
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))
(def basis (delay (b/create-basis {:project "deps.edn"})))

(defn clean [_]
  (b/delete {:path "target"}))

(def scm-url "https://github.com/multiplyco/quiescent")

(def provided-dep
  "    <dependency>
      <groupId>org.clojure</groupId>
      <artifactId>core.async</artifactId>
      <version>1.8.741</version>
      <scope>provided</scope>
    </dependency>")

(defn- add-provided-deps
  "Add provided-scope dependencies to the generated pom.xml for cljdoc analysis."
  [pom-path]
  (let [pom-content (slurp pom-path)
        updated (str/replace pom-content
                  #"(</dependencies>)"
                  (str provided-dep "\n  $1"))]
    (spit pom-path updated)))

(defn jar [_]
  (clean nil)
  (b/write-pom {:class-dir class-dir
                :lib       lib
                :version   version
                :basis     @basis
                :src-dirs  ["src"]
                :scm       {:url                 scm-url
                            :connection          (str "scm:git:" scm-url)
                            :developerConnection (str "scm:git:" scm-url)
                            :tag                 (str "v" version)}
                :pom-data  [[:licenses
                             [:license
                              [:name "Eclipse Public License 2.0"]
                              [:url "https://www.eclipse.org/legal/epl-2.0/"]]]]})
  (add-provided-deps (str class-dir "/META-INF/maven/co.multiply/quiescent/pom.xml"))
  (b/copy-dir {:src-dirs   ["src"]
               :target-dir class-dir})
  (b/copy-dir {:src-dirs   [".clj-kondo/exports"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file  jar-file})
  (println "Built:" jar-file))

(defn install [_]
  (jar nil)
  (b/install {:basis     @basis
              :lib       lib
              :version   version
              :jar-file  jar-file
              :class-dir class-dir})
  (println "Installed:" lib version))

(defn deploy [_]
  (jar nil)
  (deploy/deploy {:installer  :remote
                  :artifact   jar-file
                  :pom-file   (str class-dir "/META-INF/maven/co.multiply/quiescent/pom.xml")
                  :repository {"clojars" {:url      "https://clojars.org/repo"
                                          :username (System/getenv "CLOJARS_DEPLOY_MAVEN_USERNAME")
                                          :password (System/getenv "CLOJARS_DEPLOY_MAVEN_PASSWORD")}}}))

(defn version-str
  "Print the current version string."
  [_]
  (println version))

(defn tag
  "Create and push a version tag."
  [_]
  (let [tag (str "v" version)]
    (b/git-process {:git-args ["tag" tag]})
    (b/git-process {:git-args ["push" "origin" tag]})
    (println "Tagged and pushed:" tag)))
