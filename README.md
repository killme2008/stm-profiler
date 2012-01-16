# stm-profiler

A profiler for clojure STM.

## Usage

###Requirement

I've only test this profile tool with clojure 1.3, i will try it with clojure 1.2 soon.

###Lein dependency

		[stm-profiler "1.0.1-SNAPSHOT"]

###Usage

		(use 'stm)

It will refers some methods about STM such as dosync to a new one,it will print some warns.But please take easy,the code was copied from clojure sources and modified a little to statistics STM.And it will not change the behaviours of these functions or macros.

Now,you can start transactions to update two references:

		(def a (ref 1))
      	(def b (ref 2))
		(dotimes [_ 100] (future (dosync (alter a + 1) (alter b - 1))))

Then you can use (stm-stats) to get statistics informations:

	     => (stm-stats)
		 {"(alter a + 1)(alter b - 1)" {:not-running 9, :average-retry 15, :total-cost 3048, :get-fault 52, :barge-fail 372, :change-committed 1064, :total-times 100, :average-cost 30}}
		  
It returns a map contains all transaction forms statistics infos such as total transaction times,total retry times,the detail of retries reason and times,and transaction execution cost in milliseconds.

Also,you can use (clear-stm-stats) to clear current statistics information.		  
		  
If you want to see the statistics of a reference,you can use ref-stats function:

		 =>(ref-stats a)
		 {"(alter a + 1)(alter b - 1)" {:alter 1601, :get-fault 52, :barge-fail 372, :change-committed 1064}}		  

It also returns a map contains all transaction forms statistics infos which used this reference,and the result contains the times of special function such as alter invoked with this reference.The :alter included retry times.

You can check this example in sample.clj.

## License

Copyright (C) 2012 

Distributed under the Eclipse Public License, the same as Clojure.
