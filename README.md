# stm-profiler

A profiler for clojure STM.

## Usage

###Requirement

I've only test this profile tool with clojure 1.3, i will try it with clojure 1.2 soon.

###Lein dependency

		[stm-profiler "1.0.0-SNAPSHOT"]

###Usage

		(use 'stm)

It will refers some methods about STM such as dosync to a new one,it will print some warns.But please take easy,the code was copied from clojure sources and modified a little to statistics STM.And it will not change the behaviours of these functions or macros.

Now,you can start transactions to update two references:

		(def a (ref 1))
      	(def b (ref 2))
		(dotimes [_ 100] (future (dosync (alter a + 1) (alter b - 1))))

Then you can use (stm-stats) to get statistics informations:

	     => (stm-stats)
		 {"(alter a + 1)(alter b + 2)" {"AVERAGE_RETRY" 1, "TOTAL_COST" 610, "TOTAL_TIMES" 100, "BARGE_FAIL" 52, "CHANGE_COMMITTED" 60, "AVERAGE_COST" 6, "GET_FAULT" 10}}

It returns a map contains all transaction forms statistics infos such as total transaction times,total retry times,the detail of retries reason and times,and transaction execution cost in milliseconds.

Also,you can use (clear-stm-stats) to clear current statistics information.

You can check this example in sample.clj.

## License

Copyright (C) 2012 

Distributed under the Eclipse Public License, the same as Clojure.
