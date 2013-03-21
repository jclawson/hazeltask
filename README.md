Hazeltask
==============
An advanced task distribution system based on Hazelcast with a familiar ExecutorService API.

Hazeltask was originally designed to provide large multi-tenent SaaS applications with an easy to use general purpose work distribution platform focusing on fair customer task load balancing.  It has evolved from this mission to be much more powerful and configurable.

Winning Features
-------------
1.  Fault tolerant with task recovery
2.  Auto-balancing task allocation
3.  Configurable task prioritization
4.  ExecutorService API

FAQ
-------------
<dl>
  <dt>Why should I use this instead of Hazelcast's build in distributed ExecutorService</dt>
  <dd>Put simply, if you are not using member-targeted DistributedTasks or MultiTasks, you should absolutely use this library.  At the very least, it will give you failover recovery.  No MemberLeftExceptions.  And auto-balancing.</dd>
  <dt>What can Hazeltask NOT do, that Hazelcast's ExecutorService CAN</dt>
  <dd>The only thing Hazeltask doesn't do is handle instances of DistributedTask and MultiTask.  This is because Hazeltask does the task distribution, and it only submits a task for execution on a single member.  Its simply not part of the design.</dd>
</dl>

Features, Expanded
==============
This project encompasses an advanced distributed work library for Hazelcast.  
It is modeled after the ExecutorService API but adds a lot of missing features 
the built in Hazelcast executor service doesn't provide such as:
- Failover capabilities when nodes go down.  No lost work!
- Work distribution loadbalancing with customizable prioritizers (RoundRobin, Enum, and Fair provided... or create your own)
- Push model is light on Hazelcast with few cluster operations and no cluster locking
- No more MemberLeftExceptions when waiting on Futures!  Work will be redistributed to another member and executed returning the result to your Future
- Google Guava ListenableFuture support for easy callbacks and task chaining!

Example Use Cases:
==============

Multi-tenent Task Queue
------------------------
Let's say you have a multi-tenent system where users can submit hundreds of pages 
of scanned documents that you need to OCR.  You want to guarantee that you will OCR 
all of the pages sent, and furthermore guarantee that one user cannot starve out another.
For example, if user A submits 20,000 pages and then user B submits 1 page.  You don't want 
user B to wait a really long time before their 1 page is worked on.  You can use this library, 
group tasks by userId and use the RoundRobin router to solve this.

Priority Task Queue
----------------------
The easiest way to implement this is by grouping tasks into priorities based on an enum like HIGH, MEDIUM, LOW.  Hazeltask comes with 
built-in support for prioritizing tasks based on an enum.

Advanced Multi-tenent Priority Task Queue
------------------------------------------
Oh yes you can.  Its easy to combine the examples above and provide per-customer HIGH, MEDIUM, and LOW prioritization.  You can 
implement any kind of prioritization you can think of.  Any... priorities can CHANGE.  This allows you to implement features like:
"Bump customer 7 to the highest priority for the next hour".


Future Plans
=============
- Release the batching API branch.  Batching allows you to collect a set of items and batch them up into a task which is submitted for execution
- Allow task retries if a task fails to execute due to an exception in user code.  Retry after _ time option.
- Lite member support