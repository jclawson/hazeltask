hazelcast-work
==============
NOTE: This is a work in progress... aka... nothing is currently working...

This project encompasses an advanced distributed task library for Hazelcast.  
It is modeled after the ExecutorService API but adds a lot of missing features 
the built in Hazelcast executor service doesn't provide such as:
- Failover capabilities when nodes go down.  No lost work!
- Task loadbalancing
- Partitioned queues for customizable task execution selection
- No more MemberLeftExceptions when waiting on Futures!
- Distribution of tasks done via Hazelcast Map
- No cluster locking, or cluster wide contention on resources
- VERY light on network

Example Use Case:
==============
Let's say you have a multi-tenent system where users can submit hundreds of pages 
of scanned documents that you need to OCR.  You want to guarantee that you will OCR 
all of the pages sent, and furthermore guarantee that one user cannot starve out another.
For example, if user A submits 20,000 pages and then user B submits 1 page.  You don't want 
user B to wait a really long time before their 1 page is worked on.  You can use this library, 
partition on userId (not a hazelcast partition), and use the RoundRobin router to solve this.

