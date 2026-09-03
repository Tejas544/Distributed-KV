(ns anvil.cross
  "Runs Jepsen's Elle over histories exported by anvil_elle_export and compares
   its verdict against Anvil's own.

   Two checkers agreeing on a verdict is the claim. Two checkers agreeing on an
   anomaly *name* is not, and conflating the two would manufacture failures: a
   single cycle can legitimately be reported as G-single by one checker and
   G2-item by another depending on which minimal cycle each finds first, and
   both are correct. So naming differences are counted and printed separately
   from verdict differences, and only the verdict differences are graded."
  (:require [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.string :as str]
            [elle.list-append :as la]
            [jepsen.history :as h])
  (:gen-class))

(def ^:private aliases
  "Anomaly names that mean the same thing in the two checkers.

   These are reporting differences, not verdict differences, and each is here
   with a reason rather than because it made the numbers better:

     G-single / G-single-item   Elle distinguishes item anti-dependencies from
       predicate ones and appends the suffix; Anvil's checker is item-only by
       construction, because the list-append workload has no predicate reads at
       all. Same cycle, same edge, one of the two names is more specific.

     G2 / G2-item               the same distinction, one class up.

     duplicate-elements / duplicate-appends   see `elle-check`: Elle treats a
       repeated element as a malformed *history* and throws, where Anvil treats
       it as a reportable anomaly. Both detect it; they disagree about whose
       fault it is, which is a real difference of opinion and is recorded
       rather than smoothed over."
  {:G-single-item      :G-single
   :G-single           :G-single
   :G2-item            :G2-item
   :G2                 :G2-item
   :duplicate-appends  :duplicate-elements
   :duplicate-elements :duplicate-elements})

(defn- canonical [anomalies]
  (set (map #(get aliases % %) anomalies)))

(defn- elle-check
  "Elle's verdict for one history. Returns {:valid? bool :anomalies #{kw}}.

   :directory nil keeps Elle from writing plots for ten thousand histories."
  [history]
  ;; Elle 0.2.x wants a jepsen.history History rather than a vector of maps: it
  ;; indexes ops and pairs invocations with completions itself, and asserts on
  ;; anything else. Passing the raw vector fails with :jepsen.history/not-history
  ;; on every single case, which reads exactly like ten thousand disagreements.
  ;; Elle *throws* on a duplicate append rather than returning a verdict: it
  ;; reads a repeated element as evidence that the history itself is malformed
  ;; -- which is one of the two readings history.h names, the other being that
  ;; the database applied a write twice. Anvil takes the second reading and
  ;; reports it as an anomaly. Both detect it; catching the throw here turns a
  ;; difference of opinion about blame into what it is, rather than leaving it
  ;; to read as ten thousand crashes.
  (try
    (let [res (la/check {:consistency-models [:serializable]
                         :directory          nil}
                        (h/history history))]
      {:valid?    (true? (:valid? res))
       :anomalies (canonical (map keyword (:anomaly-types res)))})
    (catch Exception e
      (let [d (ex-data e)
            t (or (:type d) (:type (:object d)))]
        (if (= t :duplicate-appends)
          {:valid? false :anomalies #{:duplicate-elements}}
          (throw e))))))

(defn- compare-one
  [{:keys [name anvil history]}]
  (let [elle    (try (elle-check history)
                     (catch Throwable t
                       {:error (str (.getName (class t)) ": " (.getMessage t))}))
        a-valid (true? (:valid? anvil))]
    (cond
      (:error elle)
      {:name name :kind :elle-error :detail (:error elle)}

      (not= a-valid (:valid? elle))
      {:name   name
       :kind   :verdict
       :detail (str "anvil valid?=" a-valid " anomalies=" (:anomalies anvil)
                    " | elle valid?=" (:valid? elle) " anomalies=" (:anomalies elle))}

      (and (not a-valid)
           (empty? (set/intersection (canonical (:anomalies anvil)) (:anomalies elle))))
      {:name   name
       :kind   :naming
       :detail (str "both reject; anvil says " (:anomalies anvil)
                    ", elle says " (:anomalies elle))}

      :else {:name name :kind :agree})))

(defn -main
  [& args]
  (let [path  (or (first args) "/tmp/anvil-histories.edn")
        cases (edn/read-string (slurp path))
        total (count cases)]
    (println (str "read " total " histories from " path))
    ;; In chunks, and holding counters rather than results. Ten thousand Elle
    ;; analyses retained at once is several gigabytes of dependency graphs, and
    ;; the first version of this ran the heap out and exited having printed only
    ;; the line above -- which reads exactly like a crash in the checker under
    ;; test rather than in the harness driving it.
    (loop [chunks (partition-all 250 cases)
           tally  {:agree 0 :naming 0 :verdict 0 :elle-error 0}
           done   0
           shown  []]
      (if-let [chunk (first chunks)]
        (let [rs   (doall (pmap compare-one chunk))
              t    (reduce (fn [acc r] (update acc (:kind r) (fnil inc 0))) tally rs)
              bad  (filter #(#{:verdict :elle-error :naming} (:kind %)) rs)
              done (+ done (count chunk))]
          (print (str "  " done "/" total)) (flush)
          (recur (next chunks) t done (into shown (take (max 0 (- 12 (count shown))) bad))))
        (do
          (println)
          (println)
          (println (format "%-30s %d" "histories compared" total))
          (println (format "%-30s %d" "verdicts identical"
                           (+ (:agree tally) (:naming tally))))
          (println (format "%-30s %d" "  ...and same anomaly class" (:agree tally)))
          (println (format "%-30s %d" "  ...different class named" (:naming tally)))
          (println (format "%-30s %d" "VERDICT DISAGREEMENTS" (:verdict tally)))
          (println (format "%-30s %d" "elle errors" (:elle-error tally)))
          (when (seq shown)
            (println)
            (println "examples:")
            (doseq [r shown]
              (println " " (clojure.core/name (:kind r)) (:name r) "--" (:detail r))))
          (println)
          (if (and (zero? (:verdict tally)) (zero? (:elle-error tally)))
            (do (println "no unexplained disagreements with Jepsen Elle")
                (shutdown-agents)
                (System/exit 0))
            (do (println "DISAGREEMENTS FOUND -- each is a bug in exactly one of the two checkers")
                (shutdown-agents)
                (System/exit 1))))))))
