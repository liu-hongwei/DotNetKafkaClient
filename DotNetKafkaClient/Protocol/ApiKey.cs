namespace DotNetKafkaClient
{
    public enum ApiKey : short
    {
        /// <summary>
        /// Produce a message.
        /// </summary>
        Produce = 0,

        /// <summary>
        /// Fetch a message.
        /// </summary>
        Fetch = 1,

        /// <summary>
        /// Gets offsets.
        /// </summary>
        Offsets = 2,

        /// <summary>
        /// Gets topic metadata
        /// </summary>
        TopicMetadataRequest = 3,

        LeaderAndIsrRequest = 4,
        StopReplicaKey = 5,
        UpdateMetadataKey = 6,
        ControlledShutdownKey = 7,

        //-----------------------------------------\\
        //  below are consumer related apis
        //-----------------------------------------//
        OffsetCommit = 8,
        OffsetFetch = 9,
        GroupCoordinator = 10,
        JoinGroup = 11,
        Heartbeat = 12,
        LeaveGroup = 13,
        SyncGroup = 14,
        DescribeGroups = 15,
        ListGroups = 16,
    }
}