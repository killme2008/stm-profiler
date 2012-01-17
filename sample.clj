(use 'stm)
(def a (ref 1))
(def b (ref 2))

(dotimes [_ 100] (future (dosync (alter a + 1) (alter b - 1))))
(Thread/sleep 1000)
(prn @a)
(prn @b)
(Thread/sleep 1000)
(prn "stm statistics" (stm-stats))
(prn "reference a statistics" (ref-stats a))
(prn "reference b statistics" (ref-stats b))

(clear-stm-stats)
(clear-ref-stats a)
(prn "after clear")
(prn "stm statistics" (stm-stats))
(prn "reference a statistics" (ref-stats a))  