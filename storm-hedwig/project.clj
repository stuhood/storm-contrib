(defproject storm/storm-hedwig "0.1.0-SNAPSHOT"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :dependencies [[org.apache.bookkeeper/hedwig-client "4.0.0"
                   :exclusions [com.sun.jdmk/jmxtools
                                com.sun.jmx/jmxri]]
                [org.apache.bookkeeper/hedwig-protocol "4.0.0"]]
  :dev-dependencies [[storm "0.7.0"]
                     [org.clojure/clojure "1.2.0"]]
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
)
