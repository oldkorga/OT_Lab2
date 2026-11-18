using SPS.Abstractions;

namespace SPS.Student;

public sealed class Dirovskij_ReliableBrokerPolicy : ISystemPolicy
{
    private static readonly TimeSpan JobRecoveryTimeout = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan StaleHeartbeat = TimeSpan.FromMilliseconds(1500);
    private static readonly TimeSpan DemandWindow = TimeSpan.FromSeconds(6);

    private readonly TimeSpan maxJobAge;
    private readonly TimeSpan? generateBudget;
    private readonly bool alwaysMaxWorkers;
    private readonly PriorityMode priority;
    private readonly int minWorkers;
    private readonly double demandSafety;
    private readonly double patienceMaxSeconds;
    private readonly double patienceMinSeconds;

    private readonly List<(TimeSpan CreatedAt, double Work)> generateArrivals = new();
    private readonly Dictionary<string, TimeSpan> freshWindows = new(StringComparer.Ordinal);
    private readonly Dictionary<string, TimeSpan> lastAccess = new(StringComparer.Ordinal);

    private int targetWorkers = 2;
    private int lowReviews;

    public Dirovskij_ReliableBrokerPolicy(bool alwaysMaxWorkers = false, PriorityMode priority = PriorityMode.ValueScore,
        double maxJobAgeSeconds = 60, double generateBudgetSeconds = 0, int minWorkers = 2, double demandSafety = 1.5,
        double patienceMaxSeconds = 60, double patienceMinSeconds = 15)
    {
        this.alwaysMaxWorkers = alwaysMaxWorkers;
        this.priority = priority;
        this.minWorkers = Math.Max(2, minWorkers);
        this.demandSafety = Math.Max(1.0, demandSafety);
        this.patienceMaxSeconds = Math.Max(patienceMinSeconds + 1, patienceMaxSeconds);
        this.patienceMinSeconds = Math.Max(1, patienceMinSeconds);
        maxJobAge = maxJobAgeSeconds > 0 ? TimeSpan.FromSeconds(maxJobAgeSeconds) : TimeSpan.MaxValue;
        generateBudget = generateBudgetSeconds > 0 ? TimeSpan.FromSeconds(generateBudgetSeconds) : null;
    }

    public IRequestPolicy RpcPolicy { get; } = new AdaptiveRpcPolicy();

    public TimeSpan? GetProcessingBudget(RequestInfo request, Random random)
    {
        if (request.Kind == RequestKind.Generate)
            generateArrivals.Add((request.CreatedAt, request.EstimatedWorkSeconds));
        if (request.Kind == RequestKind.Generate && generateBudget is { } budget)
            return budget;
        return null;
    }

    public EndpointSelectionDecision? SelectEndpoint(RoutingContext context, Random random)
    {
        if (context.Endpoints.Count == 0) return null;

        EndpointInfo best = context.Endpoints[0];
        foreach (var endpoint in context.Endpoints)
        {
            if (endpoint.OutstandingCalls < best.OutstandingCalls ||
                endpoint.OutstandingCalls == best.OutstandingCalls && endpoint.Speed > best.Speed ||
                endpoint.OutstandingCalls == best.OutstandingCalls && endpoint.Speed == best.Speed && endpoint.Id < best.Id)
                best = endpoint;
        }
        return EndpointSelectionDecision.Assign(best.Id);
    }

    public EndpointDecision ReviewEndpoint(EndpointContext context, Random random)
    {
        var endpoint = context.Endpoint;

        if (endpoint.Role == InstanceRole.Worker)
        {
            if (endpoint.State == InstanceState.Ready && endpoint.LastHeartbeat is { } heartbeat &&
                context.Now - heartbeat > StaleHeartbeat)
                return EndpointDecision.Block();
            return EndpointDecision.Allow();
        }

        if (endpoint.Mode is EndpointMode.BlockTraffic or EndpointMode.Probe)
        {
            CallResultInfo? lastProbe = null;
            foreach (var result in context.RecentResults)
                if (result.IsProbe && (lastProbe == null || result.Time > lastProbe.Time))
                    lastProbe = result;

            if (lastProbe is { Outcome: RequestOutcome.Success } && context.Now - lastProbe.Time <= TimeSpan.FromSeconds(2))
                return EndpointDecision.Allow();
            if (lastProbe == null || context.Now - lastProbe.Time >= TimeSpan.FromMilliseconds(500))
                return EndpointDecision.Probe();
            return EndpointDecision.Block();
        }

        int streak = 0;
        for (int i = context.RecentResults.Count - 1; i >= 0; i--)
        {
            var result = context.RecentResults[i];
            if (result.IsProbe || context.Now - result.Time > TimeSpan.FromSeconds(2)) continue;
            if (result.Outcome != RequestOutcome.Success) streak++;
            else break;
        }
        return streak >= 3 ? EndpointDecision.Block() : EndpointDecision.Allow();
    }

    public CacheReadDecision OnCacheRead(CacheReadContext context, Random random)
    {
        var request = context.Request;
        lastAccess[request.ResourceKey] = context.Now;
        if (!freshWindows.TryGetValue(request.ResourceKey, out var known) || request.MaxDataAge > known)
            freshWindows[request.ResourceKey] = request.MaxDataAge;

        if (context.Entry is { } entry)
        {
            var limit = request.MaxDataAge - TimeSpan.FromMilliseconds(10);
            if (limit < TimeSpan.Zero) limit = request.MaxDataAge;
            if (context.Now - entry.SourceObservedAt <= limit && entry.ExpiresAt > context.Now)
                return CacheReadDecision.ServeCached();
        }

        if (context.PendingFetches.Count > 0)
        {
            FetchInfo? earliest = null;
            foreach (var fetch in context.PendingFetches)
                if (earliest == null || fetch.StartedAt < earliest.StartedAt)
                    earliest = fetch;
            return CacheReadDecision.JoinFetch(earliest!.Id);
        }

        return CacheReadDecision.StartFetch();
    }

    public CacheWriteDecision OnCacheFill(CacheFillContext context, Random random)
    {
        var value = context.Value;
        if (value.SizeBytes > context.CapacityBytes) return CacheWriteDecision.Skip();

        var others = new List<CacheEntryInfo>();
        long used = 0;
        foreach (var entry in context.Entries)
        {
            if (string.Equals(entry.Key, value.Key, StringComparison.Ordinal)) continue;
            others.Add(entry);
            used += entry.SizeBytes;
        }

        var evictions = new List<string>();
        if (used + value.SizeBytes > context.CapacityBytes)
        {
            others.Sort((a, b) => AccessOrder(a).CompareTo(AccessOrder(b)));
            foreach (var entry in others)
            {
                evictions.Add(entry.Key);
                used -= entry.SizeBytes;
                if (used + value.SizeBytes <= context.CapacityBytes) break;
            }
            if (used + value.SizeBytes > context.CapacityBytes) return CacheWriteDecision.Skip();
        }

        return CacheWriteDecision.Retain(ComputeTtl(value.Key), evictions.ToArray());
    }

    public JobAssignmentDecision? SelectJobAssignment(JobSchedulingContext context, Random random)
    {
        if (context.QueuedJobs.Count == 0 || context.AvailableWorkers.Count == 0) return null;

        JobInfo? chosen = null;
        foreach (var job in context.QueuedJobs)
        {
            if (context.Now - job.Request.CreatedAt > maxJobAge) continue;
            if (chosen == null || Better(job, chosen, context.Now)) chosen = job;
        }

        if (chosen == null)
        {
            JobInfo? oldest = null;
            foreach (var job in context.QueuedJobs)
                if (oldest == null || job.Request.CreatedAt < oldest.Request.CreatedAt)
                    oldest = job;
            return JobAssignmentDecision.Reject(oldest!.Id);
        }

        EndpointInfo worker = context.AvailableWorkers[0];
        foreach (var candidate in context.AvailableWorkers)
        {
            if (candidate.Speed > worker.Speed ||
                candidate.Speed == worker.Speed && candidate.AssignedJobs < worker.AssignedJobs ||
                candidate.Speed == worker.Speed && candidate.AssignedJobs == worker.AssignedJobs && candidate.Id < worker.Id)
                worker = candidate;
        }
        return JobAssignmentDecision.Assign(chosen.Id, worker.Id);
    }

    public JobRecoveryDecision ReviewJob(JobRecoveryContext context, Random random)
    {
        var job = context.Job;
        var lastSignal = job.LastHeartbeat ?? job.AssignedAt ?? job.QueuedAt;
        if (context.Now - lastSignal > JobRecoveryTimeout)
            return JobRecoveryDecision.RequeueAfter(TimeSpan.Zero);
        return JobRecoveryDecision.KeepWaiting();
    }

    public CapacityPlan ReviewCapacity(CapacityContext context, Random random)
    {
        var settings = context.Settings;
        const int apiCount = 2;

        int workerUnits = Math.Max(1, settings.Worker.ComputeUnits);
        int unitsForWorkers = (int)(settings.ComputeUnitLimit - (long)apiCount * settings.Api.ComputeUnits);
        int maxByUnits = Math.Max(2, unitsForWorkers / workerUnits);
        int maxWorkers = Math.Min(settings.MaximumWorkers, maxByUnits);

        var cutoff = context.Now - DemandWindow;
        for (int i = generateArrivals.Count - 1; i >= 0; i--)
            if (generateArrivals[i].CreatedAt < cutoff) generateArrivals.RemoveAt(i);

        double work = 0;
        foreach (var arrival in generateArrivals) work += arrival.Work;
        double workRate = work / DemandWindow.TotalSeconds;
        int demand = (int)Math.Ceiling(workRate * demandSafety);
        int backlog = Math.Min(maxWorkers, minWorkers + context.QueuedJobs.Count / 2);
        int target = alwaysMaxWorkers
            ? maxWorkers
            : Math.Clamp(Math.Max(Math.Max(minWorkers, demand), backlog), minWorkers, maxWorkers);

        if (target > targetWorkers)
        {
            targetWorkers = target;
            lowReviews = 0;
        }
        else if (target < targetWorkers)
        {
            lowReviews++;
            if (lowReviews >= 4)
            {
                targetWorkers = Math.Max(target, targetWorkers - 1);
                lowReviews = 0;
            }
        }
        else lowReviews = 0;

        if (targetWorkers > maxWorkers) targetWorkers = maxWorkers;
        return new CapacityPlan(apiCount, targetWorkers);
    }

    public void Observe(SystemObservation observation) { }

    private bool Better(JobInfo candidate, JobInfo current, TimeSpan now)
    {
        switch (priority)
        {
            case PriorityMode.Revenue:
                if (candidate.Request.Revenue != current.Request.Revenue)
                    return candidate.Request.Revenue > current.Request.Revenue;
                return candidate.Request.CreatedAt > current.Request.CreatedAt;
            case PriorityMode.RevenuePerWork:
                return Density(candidate, now) > Density(current, now);
            case PriorityMode.ValueScore:
                return Value(candidate, now) > Value(current, now);
            case PriorityMode.Youngest:
                if (candidate.Request.CreatedAt != current.Request.CreatedAt)
                    return candidate.Request.CreatedAt > current.Request.CreatedAt;
                return candidate.Request.Revenue > current.Request.Revenue;
            default:
                if (candidate.Request.CreatedAt != current.Request.CreatedAt)
                    return candidate.Request.CreatedAt < current.Request.CreatedAt;
                return candidate.Request.Revenue > current.Request.Revenue;
        }
    }

    private double Density(JobInfo job, TimeSpan now) =>
        (double)job.Request.Revenue / Math.Max(0.5, job.Request.EstimatedWorkSeconds);

    private double Value(JobInfo job, TimeSpan now)
    {
        double age = (now - job.Request.CreatedAt).TotalSeconds;
        double work = Math.Max(0.5, job.Request.EstimatedWorkSeconds);
        double remaining = patienceMaxSeconds - age - work;
        double chance = Math.Clamp(remaining / (patienceMaxSeconds - patienceMinSeconds), 0.0, 1.0);
        return (double)job.Request.Revenue * chance / work;
    }

    public enum PriorityMode { Oldest, Youngest, Revenue, RevenuePerWork, ValueScore }

    private TimeSpan AccessOrder(CacheEntryInfo entry) =>
        lastAccess.TryGetValue(entry.Key, out var access) ? access : entry.SourceObservedAt;

    private TimeSpan ComputeTtl(string key)
    {
        if (freshWindows.TryGetValue(key, out var maxAge))
        {
            var ttl = maxAge + TimeSpan.FromMilliseconds(250);
            if (ttl < TimeSpan.FromMilliseconds(500)) ttl = TimeSpan.FromMilliseconds(500);
            if (ttl > TimeSpan.FromSeconds(10)) ttl = TimeSpan.FromSeconds(10);
            return ttl;
        }
        return TimeSpan.FromSeconds(3);
    }

    private sealed class AdaptiveRpcPolicy : IRequestPolicy
    {
        private const double TargetQuantile = 0.90;
        private const double SafetyFactor = 1.3;
        private const double MinTimeoutMs = 80.0;
        private const double MaxTimeoutMs = 1000.0;
        private const double InitialTimeoutMs = 400.0;
        private const int WindowSize = 128;
        private const int MinSamples = 20;
        private const int MaxAttempts = 4;

        private readonly Queue<double> samples = new(WindowSize + 1);
        private readonly double[] scratch = new double[WindowSize];
        private double timeoutMs = InitialTimeoutMs;

        public TimeSpan GetTimeout(RequestContext context, Random random) =>
            TimeSpan.FromMilliseconds(timeoutMs);

        public RetryDecision OnResult(RequestObservation observation, Random random)
        {
            double elapsedMs = observation.Elapsed.TotalMilliseconds;

            if (observation.Outcome == RequestOutcome.Success)
            {
                AddSample(elapsedMs);
                if (samples.Count >= MinSamples)
                    timeoutMs = Math.Clamp(Percentile(TargetQuantile) * SafetyFactor, MinTimeoutMs, MaxTimeoutMs);
                return RetryDecision.NoRetry();
            }

            if (observation.Outcome == RequestOutcome.Timeout)
            {
                AddSample(Math.Max(elapsedMs, timeoutMs));
                timeoutMs = Math.Min(MaxTimeoutMs, timeoutMs * 2.0);
            }

            bool transient = observation.Outcome is RequestOutcome.Timeout or RequestOutcome.Rejected or RequestOutcome.Unavailable;
            if (!transient || observation.Attempt >= MaxAttempts) return RetryDecision.NoRetry();
            return RetryDecision.RetryAfter(observation.Outcome == RequestOutcome.Timeout
                ? TimeSpan.Zero
                : TimeSpan.FromMilliseconds(5));
        }

        private void AddSample(double value)
        {
            if (samples.Count == WindowSize) samples.Dequeue();
            samples.Enqueue(value);
        }

        private double Percentile(double quantile)
        {
            int count = samples.Count;
            int index = 0;
            foreach (double sample in samples) scratch[index++] = sample;
            Array.Sort(scratch, 0, count);
            int position = (int)Math.Ceiling(count * quantile) - 1;
            if (position < 0) position = 0;
            if (position >= count) position = count - 1;
            return scratch[position];
        }
    }
}
