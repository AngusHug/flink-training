#### 练习
1.MAP和flatMap(Exercise01 、Exercises02)
    MAP:one elem to one elem
    flatMap:one elem to zero、one or more elem

2.serializable(Exercises03)
    flink transform info with bytes. So all data must be serializable.
    Flink support to be serialized data types: ```java Tuples ,Java POJOS, Primitive Types, Regular Classes, Values, Hadoop Writables, Special Types```
    
    * POJOs requirements:
        * class must be public
        * must have a public constructor without arguments
        * All fields are either public or must be accessible through getter and setter functions
        * type of a field must bu supported by a registered serializer
3. process function(Exercises04)
    * processFunction is a low-level stream processing operation, giving access to the basic building blocks of all streaming applications:
        * events(stream elements)
        * state(fault-tolerant, consistent, only on keyed stream)
        * timers(event time and processing time,only on keyed stream)
    **The relationshipe between process function and windows? **
4. data type transform(Exercise05)
    * difference between reduce/sum/aggregations
5. charsetName(Exercise06)
    *min/minBy max/maxBy  sum/sumBy
        * min/max/sum:returns the min value from the begining until now, just the col.
            other col's value is random
        * minBy/maxBy/sumBy:return the minum value in this file
            (mean at the same line with minum value replace current line data.example [湖南])
6. process、processWindowFunction:
    * window:a window is created as soon as the first elem should belong to this window arrives.
                the window completely removed when the taime passes its end timestamp plus the user
                specifyed allowed lateness.
        a:each window have a trigger and a function attached to it.
            * the function will contain the computation to by applied to the contents of the window
            * the trigger specifies the conditions under which the window is considered ready for the 
                function to be appapplied.
            * use window:
                a:specify whether stream should be keyed or not(have to be done befor defining the window)
                b:if use keyBy:is split stream into logical keyed streams
                c:after specify keyed, we should define a window assigner.the window defines how elem are
                    assigned to windows.
                
    * ProcessWindowFunction:
        * how to define and create window???
