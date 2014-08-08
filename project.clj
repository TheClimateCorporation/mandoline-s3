(defproject io.mandoline/mandoline-s3 "0.1.0"
  :description "Amazon S3 backend for Mandoline"
  :license {:name "Apache License, version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"
            :distribution :repo}
  :min-lein-version "2.0.0"
  :url
  "https://github.com/TheClimateCorporation/mandoline-s3"
  :mailing-lists
  [{:name "mandoline-users@googlegroups.com"
    :archive "https://groups.google.com/d/forum/mandoline-users"
    :post "mandoline-users@googlegroups.com"}
   {:name "mandoline-dev@googlegroups.com"
    :archive "https://groups.google.com/d/forum/mandoline-dev"
    :post "mandoline-dev@googlegroups.com"}]
  :checksum :warn
  :dependencies
  [[amazonica "0.2.16"]
   [environ "0.5.0"]
   [io.mandoline/mandoline-core "0.1.3"]
   [joda-time/joda-time "2.1"]
   [log4j "1.2.17"]
   [org.clojure/clojure "1.5.1"]
   [org.clojure/tools.logging "0.2.6"]
   [org.clojure/core.cache "0.6.3"]
   [org.clojure/core.memoize "0.5.6"]
   [org.slf4j/slf4j-log4j12 "1.7.2"]]
  :exclusions [org.clojure/clojure]

  :profiles {
    :dev {:dependencies
           [[lein-marginalia "0.7.1"]]}}

  :aliases {"docs" ["marg" "-d" "target"]
            "package" ["do" "clean," "jar"]}

  :plugins [[lein-marginalia "0.7.1"]
            [lein-cloverage "1.0.2"]]
  :test-selectors {:default #(not-any? % [:integration])
                   :integration :integration
                   :local :local
                   :all (constantly true)})
