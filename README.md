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
