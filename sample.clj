(use 'stm)
(def a (ref 1))
(def b (ref 2))

(dotimes [_ 100] (future (dosync (alter a + 1) (alter b + 2))))

(prn @a)
(prn @b)
(Thread/sleep 1000)
(prn (stm-stats))