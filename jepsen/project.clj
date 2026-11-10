(defproject turbofan-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen tests of a Turbofan cluster: list-append, checked by Elle (#14)."
  :license {:name "Apache-2.0"}
  :main turbofan.jepsen
  :jvm-opts ["-Xmx4g" "-Djava.awt.headless=true"]
  :dependencies [[org.clojure/clojure "1.12.6"]
                 [jepsen "0.3.15"]])
