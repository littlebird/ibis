# ibis

Distribute tasks among any number of peers through Kafka

![IBIS](https://github.com/littlebird/ibis/blob/master/resources/public/img/ibis.jpg)

## Usage

Ibis is a library for managing the lifecycle of streaming tasks in a distributed environment, and relies on Kafka and Zookeeper to communicate and keep track of the progress of these tasks.

The main concepts are that of the Journey, which goes through many Stages according to an acyclic directed graph defined by the Course the Journey takes.  These ideas are explained below.

To include Ibis in your project add this line to the dependencies of your `project.clj`:

```
[littlebird/ibis "0.0.13"]
```

### The Stages of the Journey

Every Journey is defined by the Stages it goes through.  When initializing Ibis, you pass in all of the Stages that all of the Journeys your system will take may use.  A Stage is really just a Clojure function that takes a map and returns a map with a key designating it.  This way you can define your Courses with data, rather than passing functions around.

Here is an example of a map of Stages you might pass to Ibis on startup:

```clj
(def stages
  {:inc (fn [m] (update m :n inc))
   :add-five (fn [m] (update m :n (partial + 5)))
   :str (fn [{:keys [n]}] {:s (str n)})})
```

Now we are ready to define the Course our Journey will take.

### The Course of the Journey

The Stages of the Journey we defined earlier provide our system with the functions it will need to do its work, but the order and the flow of data through the system is defined by the Course, and can be changed for each Journey.  Here are three different courses that could be defined from the Stages we defined earlier:

```clj
(def simple-course
  {:in [:inc]
   :inc [:out]})

(def branching-course
  {:in [:inc :add-five]
   :inc [:str]
   :add-five [:str]
   :str [:out]})

(def complex-course
  {:in [:inc :add-five :str]
   :inc [:add-five :str :out]
   :add-five [:str :out]
   :str [:out]})
```

Notice that there are two Stages present in these Course examples that we did not define, `:in` and `:out`.  These are special reserved Stages that specify where data comes in and out of the system.

* `:in` is where data enters the data flow defined by our Course
* `:out` is where data that has flowed through the Stages of the Journey finish processing and should be sent to output.

There is a lot of flexibility about how to define this data flow, but one thing we should never do is create a loop in this graph.  

Now that we have a Course, time to start the Journey.

### Launching Ibis

Once you have the Stages you will need on your Journey, you can start Ibis:

```clj
(require '[ibis.core :as ibis])

(def ibis (ibis/start {:stages stages}))
```

There are a number of other options you can pass into `ibis/start` to configure your Ibis instance.  We explain these below in the section `Configuring Ibis`.

Now that we have an Ibis instance, we have control over how many threads are committed to processing Ibis tasks.  These threads are collectively referred to as the Flock.

```clj
(require '[ibis.flock :as flock])

(flock/launch-all! ibis 15)
```

This launches 15 threads all waiting for Ibis messages to come through Kafka.  If you like, you can dynamically add more threads during runtime by calling `flock/launch!`:

```clj
(flock/launch! ibis)
```

Each time you do this another thread will be added to the Flock.

### The Journey

To start a Journey, we must have a Course which is our map for data flowing through the given Stages that Ibis is aware of.  Given what we have done so far, we are now ready to start a Journey:

```clj
(require '[ibis.journey :as journey])

(def journey (journey/submit! ibis branching-course))
```

Now that the Journey has begun, it is ready to accept input.  

Ibis is modeled as a streaming data processing system, so you can add data at any time using the `journey/push!` function.

```clj
(journey/push! ibis journey {:n 1})
(journey/push! ibis journey {:n 5})
(journey/push! ibis journey {:n 88})
```

Each Journey will continue to accept input for as long as you want to provide it.  When you are done, just call `journey/finish!`:

```clj
(journey/finish! ibis journey)
```

Now you can pull in the results of this journey with `journey/pull!`.  This will block until all of the results are available.  Provide a reducer function and an initial value and revel in your results:

```clj
(journey/pull! ibis journey conj [])

---> [{:s "6"} {:s "10"} {:s "6"} {:s "2"} {:s "89"} {:s "93"}]
```

### Streaming Journey

If you do not want to worry about the Journey finishing before you get results, you can pass a `core.async` channel into `pull!` to get results as they come in:

```clj
(require '[clojure.core.async :as >])

(def streaming-chan (>/chan)) ;; make a channel for streaming
(def pull-results (future (journey/pull! ibis journey conj [] streaming-chan))) ;; pass the channel in
(>/go-loop []
  (let [result (>/<! streaming-chan)] ;; get results one at a time
    (println result)
    (recur)))

@pull-results ;; wait for all results
```

### Scheduling Journeys

Some things need to happen on a regular basis.  For this need Ibis provides a scheduler which can be used to start Journeys at periodic intervals.

Once you have started ibis, in the resulting component map there is a function that lives under the `:schedule` key which can be used to schedule Journeys to occur periodically:

```clj
(require '[clj-time.core :as time])

(defn periodic-journey
  [push!]
  (doseq [n (range 15)]
    (push! {:n n})))

(def schedule (:schedule ibis))
(def stop
  (schedule
    ibis ;; pass in the ibis instance
    (time/plus (time/now) (time/hours 3)) ;; give the scheduler a start time, here 3 hours from now
    (time/hours 1) ;; tell it how often the journey should be run
    branching-course ;; the course for the periodic journey
    periodic-journey)) ;; the function to run periodically, which pushes the data into the journey
```

The function you pass as the last argument to `schedule` will be called with a single argument, `push!`.  This is a partial of `ibis.journey/push!` (with `ibis` and `journey` bound) which pushes each given piece of data onto the input for the Journey, as defined by the Course supplied in the previous argument to `schedule`.

You don't need to worry about calling `journey/submit!` or `journey/finish!` or even `journey/pull!`.  This is all taken care of by the scheduler.  If you do want some results from these periodic Journeys (as in, they are not just run for side effects), you can pass in a `core.async` channel as the final argument and any results will be passed to you as they come in.

### Uniquely Calling a Function

Sometimes in a distributed environment with many nodes all running the same code, you want something to just happen once!  If all the boxes have the same code, how do you run something and make sure it happens only once across all instances, rather than once for each box running the code?

Enter `ibis.tempo/uniquely!`:

```clj
(require '[ibis.tempo :as tempo])

(defn singularity
  []
  (println "Trouble if this runs more than once"))

(tempo/uniquely!
  ibis
  ["path" "that" "uniquely" "identifies" "call"]
  singularity)
```

Using Zookeeper, we can guarantee this function is only ever run once per bootup, no matter how many nodes try to run it any number of times.  If you shut the process down and restart it, it will run again however.  This is to establish singular processes that are only occuring in one instance at a time.  

### Configuring Ibis

Ibis accepts a number of configuration options.  Here are the possible options with their default values:

```clj
(def default-config
  {:stages {}
   :zookeeper-host "127.0.0.1"
   :zookeeper-port "2181"
   :kafka-port "9092"
   :group "ibis"
   :topic "ibis-journeys"
   :encoders {}
   :decoders {}
   :producer-opts {}
   :consumer-opts {}})
```

## License

Copyright © 2015 Littlebird

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
