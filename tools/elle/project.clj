;; The Jepsen Elle cross-validation harness -- P7 exit criterion 2.
;;
;; This is the only gate in the repository that is not Anvil checking Anvil.
;; Everything else -- the mutation score, the state-space search, the TLA+
;; specs -- is our own understanding written down more than once, which is
;; worth doing and cannot catch a mistake we made consistently. Elle shares no
;; ancestry with anvil/checker/elle.cc; it was written by other people and has
;; found real anomalies in real databases. Agreeing with it is the only
;; evidence here that is not self-referential.
;;
;;   lein run -- /tmp/anvil-histories.edn
;;
;; The histories come from anvil_elle_export, which also attaches Anvil's own
;; verdict, so one file carries both halves of the comparison.

(defproject anvil-elle-cross "0.1.0"
  :description "Cross-validates anvil/checker/elle.cc against Jepsen's Elle"
  :dependencies [[org.clojure/clojure "1.11.3"]
                 [elle "0.2.4"]
                 [spootnik/unilog "0.7.32"]]
  :main anvil.cross
  :jvm-opts ["-Xmx8g" "-server"])
