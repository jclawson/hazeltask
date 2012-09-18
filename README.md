Hazeltask
==============
An advanced task distribution system based on Hazelcast with a familiar ExecutorService API

Current Status
==============
Almost to 1.0!  This 0.9 release is functional, just a few TODO's left!

Features
==============
This project encompasses an advanced distributed work library for Hazelcast.  
It is modeled after the ExecutorService API but adds a lot of missing features 
the built in Hazelcast executor service doesn't provide such as:
- Failover capabilities when nodes go down.  No lost work!
- Work distribution loadbalancing with customizable routers (RoundRobin implementation provided)
- Local Partitioned queues for customizable task execution selection
- No more MemberLeftExceptions when waiting on Futures!  Work will be redistributed to another member and executed returning the result to your Future
- Distribution of work done via push as work is added
- No cluster locking.  Little contention on cluster wide resources.
- Configurable to favor speed or redundancy
- Work Bundling capabilities!  Sometimes its more efficient to combine several tasks into a single task.  This is now possible with the DistributedWorkBundler.

Example Use Case:
==============
Let's say you have a multi-tenent system where users can submit hundreds of pages 
of scanned documents that you need to OCR.  You want to guarantee that you will OCR 
all of the pages sent, and furthermore guarantee that one user cannot starve out another.
For example, if user A submits 20,000 pages and then user B submits 1 page.  You don't want 
user B to wait a really long time before their 1 page is worked on.  You can use this library, 
partition on userId (not a hazelcast partition), and use the RoundRobin router to solve this.

