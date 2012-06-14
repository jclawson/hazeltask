package com.succinctllc.hazelcast.work.executor;


/**
 * There must only be 1 of these per topologyName
 * 
 * TODO: I want to merge this class into the DistributedExecutorService and Topology
 * 
 * @author jclawson
 * 
 */
@Deprecated
public class DistributedExecutorServiceManager {
/*
    private static ConcurrentMap<String, DistributedExecutorServiceManager> serviceManagersByTopology = new ConcurrentHashMap<String, DistributedExecutorServiceManager>();

    private final int                                                       serviceIndex;
    private final String                                                    topologyName;
    private final DistributionType                                          type;

    private final LocalWorkExecutorService                                  localExecutorService;
    private final DistributedExecutorService                                       distributedExecutorService;

    private final IMap<String, HazelcastWork>                               map;

    // TODO: implements this later as a possible optimization for when nodes go
    // down
    // private final MultiMap<Member, HazelcastWork> memberWork;
    private final ExecutorService                                           workDistributor;

    // a member must indicate that it is ready to receive work in order for work
    // to be given to it
    private final CopyOnWriteArrayListSet<Member>                           readyMembers              = new CopyOnWriteArrayListSet<Member>();
    private final ExecutorService                                           communicationExecutorService;

    public ExecutorService getCommunicationExecutorService() {
        return communicationExecutorService;
    }

    public ExecutorService getWorkDistributorService() {
        return this.workDistributor;
    }

    private volatile boolean         isReady  = false;

    private final WorkKeyAdapter     partitionAdapter;
    private final UUID               jvmId    = UUID.randomUUID();
    private final long               jvmNanos = System.nanoTime();
    private final HazelcastInstance  hazelcast;
    private final ListRouter<Member> memberRouter;

    public ListRouter<Member> getMemberRouter() {
        return memberRouter;
    }

    protected DistributedExecutorServiceManager(InternalBuilderStep2 internalBuilderStep1) {
        this.serviceIndex = internalBuilderStep1.serviceIndex;
        this.topologyName = internalBuilderStep1.topologyName;
        this.type = internalBuilderStep1.type;
        this.partitionAdapter = internalBuilderStep1.partitionAdapter;
        this.hazelcast = internalBuilderStep1.hazelcast;

        // executor services
        communicationExecutorService = hazelcast.getExecutorService(buildName("com"));

        String workDistributorSvcName = buildName("work-distributor");

        workDistributor = hazelcast.getExecutorService(workDistributorSvcName);

        this.map = hazelcast.getMap(buildName("work"));
        // map.addIndex(attribute, ordered) add index for timestamp so we can
        // query for old items

        // add long TTL for the work map to expire elements if they are not
        // touched after so long...
        // hazelcast.getConfig().addMapConfig(mapConfig)
        this.localExecutorService = new LocalWorkExecutorService(this);
        this.distributedExecutorService = new DistributedExecutorService(this);

        if (serviceManagersByTopology.putIfAbsent(topologyName, this) != null) { throw new IllegalArgumentException(
                "A DistributedExecutorServiceManager already exists for the topology "
                        + topologyName); }

        memberRouter = new RoundRobinRouter<Member>(new Callable<List<Member>>() {
            public List<Member> call() throws Exception {
                return getAvailableMembers();
            }
        });

    }

    public void start() {
        this.getLocalExecutorService().start();
        isReady = true;
        new Timer(buildName("flush-timer"), true)
            .schedule(new StaleItemsFlushTimerTask(this), 6000, 6000);
    }

    

    private void checkReadyMembers() {
        // MultiTask<Boolean> task = new MultiTask<Boolean>(new
        // IsReady(topologyName), this.hazelcast.getCluster().getMembers());
        Collection<MemberResponse<Boolean>> results = MemberTasks.executeOptimistic(
                this.communicationExecutorService, this.hazelcast.getCluster().getMembers(),
                new IsReady(topologyName));

        Member thisMember = hazelcast.getCluster().getLocalMember();

        for (MemberResponse<Boolean> result : results) {
            if (result.getValue()) {
                Member m = result.getMember();
                // we need to make sure the member things its local if it is
                // hazelcast is dumb
                if (m.equals(thisMember)) m = thisMember;
                this.readyMembers.add(m);
            }
        }
    }

    private volatile long lastChecked = 0;

   
    protected CopyOnWriteArrayListSet<Member> getAvailableMembers() {
        // FIXME: put this in another thread with an exponential backoff
        // this is temporary
        if ((System.currentTimeMillis() - lastChecked) >= 10000) { // check
                                                                   // every 10
                                                                   // seconds
                                                                   // for ready
                                                                   // members
            lastChecked = System.currentTimeMillis();
            this.checkReadyMembers();
        }

        // TODO: don't do this check every time?
        Set<Member> onlineMembers = this.hazelcast.getCluster().getMembers();
        this.readyMembers.retainAll(onlineMembers);
        return this.readyMembers;
    }

    public static DistributedExecutorServiceManager getDistributedExecutorServiceManager(
            String topology) {
        return serviceManagersByTopology.get(topology);
    }

    private String buildName(String postfix) {
        return this.topologyName + "-" + postfix;
    }

    public int getServiceIndex() {
        return serviceIndex;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public DistributionType getType() {
        return type;
    }

    public LocalWorkExecutorService getLocalExecutorService() {
        return localExecutorService;
    }

    public DistributedExecutorService getDistributedExecutorService() {
        return distributedExecutorService;
    }

    public IMap<String, HazelcastWork> getMap() {
        return map;
    }

    public WorkKeyAdapter getPartitionAdapter() {
        return partitionAdapter;
    }

    public UUID getJvmId() {
        return jvmId;
    }

    public long getJvmNanos() {
        return jvmNanos;
    }

    public HazelcastInstance getHazelcast() {
        return hazelcast;
    }

	public boolean isReady() {
		return isReady;
	}
    
    */

}
